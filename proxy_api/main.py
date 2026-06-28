"""
Databricks Proxy API
=====================
EC2에서 실행되는 FastAPI 서버.
Sandbox(Claude) → EC2 프록시 → Databricks API 경로를 제공.

엔드포인트:
  GET  /health              - 서버 상태 확인
  GET  /clusters            - 클러스터 목록
  POST /sql                 - SQL 실행 (Databricks SQL Warehouse)
  POST /pyspark             - PySpark 코드 실행 (클러스터)
  GET  /pyspark/{cmd_id}    - PySpark 실행 결과 조회
"""

import os
import time
import httpx
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv("/home/ubuntu/jaffle-shop-analytics/.env")

app = FastAPI(title="Databricks Proxy API", version="1.0.0")
security = HTTPBearer()

# ── 환경변수 ───────────────────────────────────────────────
DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
PROXY_SECRET         = os.environ["PROXY_SECRET"]

# Warehouse ID: HTTP path에서 추출 (/sql/1.0/warehouses/<id>)
WAREHOUSE_ID = DATABRICKS_HTTP_PATH.split("/")[-1]

DB_HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
DB_BASE    = f"https://{DATABRICKS_HOST}"


# ── 인증 ───────────────────────────────────────────────────
def verify(creds: HTTPAuthorizationCredentials = Depends(security)):
    if creds.credentials != PROXY_SECRET:
        raise HTTPException(status_code=401, detail="Invalid proxy token")
    return creds


# ── 요청 모델 ──────────────────────────────────────────────
class SQLRequest(BaseModel):
    sql: str
    timeout: int = 30  # 초 (Databricks 제한: 5~50초)

class PySparkRequest(BaseModel):
    code: str
    cluster_id: str


# ── 엔드포인트 ─────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "databricks_host": DATABRICKS_HOST}


@app.get("/clusters", dependencies=[Depends(verify)])
async def list_clusters():
    """전체 클러스터 목록 조회 (상태 무관)"""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{DB_BASE}/api/2.0/clusters/list", headers=DB_HEADERS)
    data = resp.json()
    clusters = [
        {
            "cluster_id": c["cluster_id"],
            "cluster_name": c["cluster_name"],
            "state": c["state"],
        }
        for c in data.get("clusters", [])
    ]
    return {"clusters": clusters}


class ClusterCreateRequest(BaseModel):
    cluster_name: str = "claude-pyspark"
    spark_version: str = "15.4.x-scala2.12"   # Databricks Runtime LTS
    node_type_id: str = "i3.xlarge"
    num_workers: int = 1
    autotermination_minutes: int = 30


@app.post("/clusters/create", dependencies=[Depends(verify)])
async def create_cluster(req: ClusterCreateRequest):
    """새 클러스터 생성"""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{DB_BASE}/api/2.0/clusters/create",
            headers=DB_HEADERS,
            json={
                "cluster_name": req.cluster_name,
                "spark_version": req.spark_version,
                "node_type_id": req.node_type_id,
                "num_workers": req.num_workers,
                "autotermination_minutes": req.autotermination_minutes,
                "spark_conf": {
                    "spark.databricks.cluster.profile": "singleNode"
                    if req.num_workers == 0 else ""
                },
            },
        )
    return resp.json()


@app.post("/clusters/start/{cluster_id}", dependencies=[Depends(verify)])
async def start_cluster(cluster_id: str):
    """클러스터 시작"""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{DB_BASE}/api/2.0/clusters/start",
            headers=DB_HEADERS,
            json={"cluster_id": cluster_id},
        )
    return resp.json()


@app.get("/clusters/{cluster_id}", dependencies=[Depends(verify)])
async def get_cluster(cluster_id: str):
    """클러스터 상태 조회"""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{DB_BASE}/api/2.0/clusters/get",
            headers=DB_HEADERS,
            params={"cluster_id": cluster_id},
        )
    data = resp.json()
    return {
        "cluster_id": data.get("cluster_id"),
        "cluster_name": data.get("cluster_name"),
        "state": data.get("state"),
        "state_message": data.get("state_message", ""),
        "spark_version": data.get("spark_version"),
        "node_type_id": data.get("node_type_id"),
    }


@app.post("/sql", dependencies=[Depends(verify)])
async def execute_sql(req: SQLRequest):
    """Databricks SQL Warehouse에서 SQL 실행"""
    async with httpx.AsyncClient(timeout=req.timeout + 10) as client:
        resp = await client.post(
            f"{DB_BASE}/api/2.0/sql/statements/",
            headers=DB_HEADERS,
            json={
                "warehouse_id": WAREHOUSE_ID,
                "statement": req.sql,
                "wait_timeout": f"{req.timeout}s",
                "format": "JSON_ARRAY",
            },
        )
    result = resp.json()

    # 결과 파싱
    status = result.get("status", {}).get("state")
    if status == "SUCCEEDED":
        schema = [c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = result.get("result", {}).get("data_array", [])
        return {"status": "SUCCEEDED", "columns": schema, "rows": rows, "row_count": len(rows)}
    else:
        error = result.get("status", {}).get("error", {})
        return {"status": status, "error": e