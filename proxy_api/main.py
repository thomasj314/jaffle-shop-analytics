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
    timeout: int = 60  # 초

class PySparkRequest(BaseModel):
    code: str
    cluster_id: str


# ── 엔드포인트 ─────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "databricks_host": DATABRICKS_HOST}


@app.get("/clusters", dependencies=[Depends(verify)])
async def list_clusters():
    """실행 중인 클러스터 목록 조회"""
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
        return {"status": status, "error": error.get("message", str(result))}


@app.post("/pyspark", dependencies=[Depends(verify)])
async def execute_pyspark(req: PySparkRequest):
    """클러스터에서 PySpark 코드 실행 (비동기 — command_id 반환)"""
    async with httpx.AsyncClient(timeout=30) as client:
        # 실행 컨텍스트 생성
        ctx_resp = await client.post(
            f"{DB_BASE}/api/1.2/contexts/create",
            headers=DB_HEADERS,
            json={"clusterId": req.cluster_id, "language": "python"},
        )
        ctx = ctx_resp.json()
        if "id" not in ctx:
            raise HTTPException(status_code=400, detail=f"Context 생성 실패: {ctx}")

        # 코드 실행
        cmd_resp = await client.post(
            f"{DB_BASE}/api/1.2/commands/execute",
            headers=DB_HEADERS,
            json={
                "clusterId": req.cluster_id,
                "contextId": ctx["id"],
                "language": "python",
                "command": req.code,
            },
        )
        cmd = cmd_resp.json()

    return {
        "command_id": cmd.get("id"),
        "context_id": ctx["id"],
        "cluster_id": req.cluster_id,
        "status": "submitted",
    }


@app.get("/pyspark/{cluster_id}/{context_id}/{command_id}", dependencies=[Depends(verify)])
async def get_pyspark_result(cluster_id: str, context_id: str, command_id: str):
    """PySpark 실행 결과 조회"""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{DB_BASE}/api/1.2/commands/status",
            headers=DB_HEADERS,
            params={
                "clusterId": cluster_id,
                "contextId": context_id,
                "commandId": command_id,
            },
        )
    result = resp.json()
    status = result.get("status")

    if status == "Finished":
        results = result.get("results", {})
        return {
            "status": "Finished",
            "result_type": results.get("resultType"),
            "data": results.get("data"),
        }
    elif status == "Error":
        return {"status": "Error", "cause": result.get("results", {}).get("cause")}
    else:
        return {"status": status}  # Running, Queued 등
