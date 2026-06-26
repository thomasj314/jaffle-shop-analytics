"""
Jaffle Shop Dagster Assets
===========================
파이프라인 흐름:
  synthetic_data (S3 업로드)
      ↓
  raw_orders / raw_payments / raw_customers  (Bronze COPY INTO)
      ↓  (dbt sources 자동 연결)
  stg_* / silver.orders / silver.payments    (dbt run)
      ↓
  customers_snapshot                          (dbt snapshot → SCD2)

💡 R1 (멱등성):
  - COPY INTO: 파일 처리 이력을 내부 추적 → 중복 적재 없음
  - dbt: unique_key 기반 테이블 재생성 → 동일 결과 보장
  - 합성 데이터: 타임스탬프 파일명 → 재실행해도 새 파일로 구분
"""

import os
import subprocess
import sys
from pathlib import Path

from dagster import asset, AssetExecutionContext, Definitions, Output
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# ─────────────────────────────────────────────────────────────
# 경로 / 접속 정보
# ─────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = Path(__file__).parent.parent  # jaffle_shop/

# Windows / Linux 모두 동작하는 Python 실행파일 경로
PYTHON_EXE = sys.executable

# 🚩 접속 정보는 환경변수로 관리 — .env 파일에 넣고 .gitignore에 추가할 것
# 로컬 개발: jaffle_shop/.env 파일에 아래 변수 정의
# 서버 배포: 서버의 환경변수 또는 Secret Manager에 등록
DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]


# ─────────────────────────────────────────────────────────────
# Databricks SQL 실행 헬퍼
# ─────────────────────────────────────────────────────────────
def run_databricks_sql(query: str):
    """
    databricks-sql-connector로 SQL 실행.
    💡 Dagster에서는 보통 Resource로 분리하지만,
       학습 목적으로 간단히 함수로 구현.
    """
    from databricks import sql
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            try:
                return cursor.fetchall()
            except Exception:
                return []


def copy_into_bronze(context: AssetExecutionContext, table: str, s3_path: str, columns: list[str]) -> int:
    """
    COPY INTO 실행 → 신규 파일만 읽어 Bronze 테이블에 적재.

    💡 R1 멱등성: COPY INTO는 처리한 파일 경로를 Delta 트랜잭션 로그에 기록.
       같은 파일을 두 번 실행해도 두 번째는 0건 적재 → 중복 없음.
    """
    cols = ", ".join(columns)
    result = run_databricks_sql(f"""
        COPY INTO jaffle_shop.bronze.{table}
        FROM (
          SELECT {cols},
                 _metadata.file_modification_time AS _ingested_at,
                 _metadata.file_path              AS _source_file
          FROM '{s3_path}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
        COPY_OPTIONS   ('mergeSchema' = 'false')
    """)
    rows_inserted = result[0][1] if result else 0
    context.log.info(f"[bronze.{table}] 신규 적재: {rows_inserted}행")
    return rows_inserted


# ═════════════════════════════════════════════════════════════
# LAYER 0: 합성 데이터 생성  (Ingestion)
# ═════════════════════════════════════════════════════════════

@asset(
    group_name="ingestion",
    description="새 주문 15건 + 고객 이름 변경 3건 생성 → S3 업로드",
)
def synthetic_data(context: AssetExecutionContext):
    """
    실행할 때마다 타임스탬프 파일명으로 새 CSV 생성 → S3 업로드.

    💡 정적 Jaffle Shop 데이터의 한계를 극복하기 위한 장치.
       실제 프로덕션에서는 소스 DB의 CDC(Change Data Capture) 또는
       Kafka 스트림이 이 역할을 담당.
    """
    script = DBT_PROJECT_DIR / "synthetic_data_generator.py"
    proc = subprocess.run(
        [PYTHON_EXE, str(script), "--orders", "15", "--customer-changes", "3"],
        capture_output=True, text=True, cwd=str(DBT_PROJECT_DIR),
    )
    context.log.info(proc.stdout)
    if proc.returncode != 0:
        raise Exception(f"synthetic_data_generator 실패:\n{proc.stderr}")
    return Output(True, metadata={"log": proc.stdout})


# ═════════════════════════════════════════════════════════════
# LAYER 1: Bronze COPY INTO
# ═════════════════════════════════════════════════════════════

@asset(
    deps=[synthetic_data],
    key_prefix="bronze",   # AssetKey = ["bronze", "raw_orders"] → dbt source 자동 연결
    group_name="bronze",
    description="S3 raw/orders/ → bronze.raw_orders  (COPY INTO, 중복 없음)",
)
def raw_orders(context: AssetExecutionContext):
    rows = copy_into_bronze(
        context, "raw_orders",
        "s3://jaffleshopclassic-usw2/raw/orders/",
        ["id", "user_id", "order_date", "status"],
    )
    return Output(rows, metadata={"rows_inserted": rows})


@asset(
    deps=[synthetic_data],
    key_prefix="bronze",
    group_name="bronze",
    description="S3 raw/payments/ → bronze.raw_payments",
)
def raw_payments(context: AssetExecutionContext):
    rows = copy_into_bronze(
        context, "raw_payments",
        "s3://jaffleshopclassic-usw2/raw/payments/",
        ["id", "order_id", "payment_method", "amount"],
    )
    return Output(rows, metadata={"rows_inserted": rows})


@asset(
    deps=[synthetic_data],
    key_prefix="bronze",
    group_name="bronze",
    description="S3 raw/customers/ → bronze.raw_customers (이름 변경 델타 포함)",
)
def raw_customers(context: AssetExecutionContext):
    rows = copy_into_bronze(
        context, "raw_customers",
        "s3://jaffleshopclassic-usw2/raw/customers/",
        ["id", "first_name", "last_name"],
    )
    return Output(rows, metadata={"rows_inserted": rows})


# ═════════════════════════════════════════════════════════════
# LAYER 2 + 3: Silver dbt (run + snapshot)
# ═════════════════════════════════════════════════════════════

# 📚 DbtProject: manifest.json 경로를 자동 추적.
#    @dbt_assets가 manifest를 읽어 dbt 모델을 Dagster Asset으로 자동 변환.
#    key_prefix="bronze"인 Asset(raw_orders 등)과 dbt sources.yml의
#    bronze.raw_* 이 AssetKey로 자동 매칭 → 의존성 그래프 자동 완성.
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)


@dbt_assets(manifest=dbt_project.manifest_path)
def jaffle_shop_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt_project.yml의 모든 모델을 Dagster Asset으로 자동 생성.

    💡 @dbt_assets는 manifest.json을 파싱해서:
       - 각 dbt 모델 → 개별 Dagster Asset
       - ref() 관계 → Asset 의존성
       - source() → 업스트림 Asset 연결 (bronze.raw_* 와 자동 매핑)

    ⚖️ 대안: dbt run을 subprocess로 직접 호출할 수도 있지만,
       @dbt_assets는 모델별 실패/성공 추적과 lineage를 Dagster UI에 표시.

    순서: dbt run (Silver 테이블) → dbt snapshot (SCD2 이력)
    """
    # Silver 모델 실행 (stg_* views + silver.orders, silver.payments tables)
    context.log.info("▶ dbt run 시작 (Silver 모델)")
    yield from dbt.cli(["run"], context=context).stream()

    # SCD2 Snapshot (customers_snapshot)
    context.log.info("▶ dbt snapshot 시작 (SCD2 이력)")
    yield from dbt.cli(["snapshot"], context=context).stream()


# ═════════════════════════════════════════════════════════════
# Definitions — Dagster가 인식하는 최상위 등록 객체
# ═════════════════════════════════════════════════════════════
defs = Definitions(
    assets=[
        synthetic_data,
        raw_orders,
        raw_payments,
        raw_customers,
        jaffle_shop_dbt_assets,
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(Path.home() / ".dbt"),
        ),
    },
)
