"""
Dagster Definitions
====================
Dagster가 인식할 전체 구성 등록:
  - Assets   : 파이프라인의 각 단계 + 비즈니스 DQ 체크
  - Resources: dbt CLI 설정
  - Jobs     : 전체 파이프라인 / DQ 체크 잡
  - Schedules: 매일 자동 실행
"""

import shutil
from pathlib import Path
from dotenv import load_dotenv

# .env 파일 로드 (로컬 개발용 — 서버 환경에서는 시스템 환경변수 사용)
load_dotenv(Path(__file__).parent.parent / ".env")

from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
)
from dagster_dbt import DbtCliResource

from dagster_project.assets import (
    synthetic_data,
    raw_orders,
    raw_payments,
    raw_customers,
    jaffle_shop_dbt_assets,
)
from dagster_project.dq_checks import (
    dq_gold_row_counts,
    dq_revenue_anomaly,
    dq_return_rate,
    dq_aov_range,
    dq_new_customers,
    dq_job,
    dq_schedule,
)

DBT_PROJECT_DIR  = Path(__file__).parent.parent
DBT_PROFILES_DIR = str(Path.home() / ".dbt")
DBT_EXE          = shutil.which("dbt") or str(Path.home() / ".local" / "bin" / "dbt")

# ─────────────────────────────────────────────────────────────
# Job: 전체 파이프라인
# ─────────────────────────────────────────────────────────────
jaffle_shop_job = define_asset_job(
    name="jaffle_shop_pipeline",
    selection=AssetSelection.all() - AssetSelection.groups("dq_business"),
    description="합성 데이터 생성 → Bronze 적재 → Silver dbt → SCD2 Snapshot",
)

# 매일 오전 10시 KST (UTC 01:00)
daily_schedule = ScheduleDefinition(
    job=jaffle_shop_job,
    cron_schedule="0 1 * * *",
    name="daily_jaffle_shop",
)

# ─────────────────────────────────────────────────────────────
# Definitions: Dagster 진입점
# ─────────────────────────────────────────────────────────────
defs = Definitions(
    assets=[
        # 파이프라인
        synthetic_data,
        raw_orders,
        raw_payments,
        raw_customers,
        jaffle_shop_dbt_assets,
        # 비즈니스 DQ 체크
        dq_gold_row_counts,
        dq_revenue_anomaly,
        dq_return_rate,
        dq_aov_range,
        dq_new_customers,
    ],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=DBT_PROFILES_DIR,
            dbt_executable=DBT_EXE,
        ),
    },
    jobs=[jaffle_shop_job, dq_job],
    schedules=[daily_schedule, dq_schedule],
)