"""
Dagster Definitions
====================
Dagster가 인식할 전체 구성 등록:
  - Assets   : 파이프라인의 각 단계
  - Resources: dbt CLI 설정
  - Job      : 전체 파이프라인을 하나의 실행 단위로 묶음
  - Schedule : 매일 자동 실행

💡 Definitions는 Dagster의 "진입점". dagster dev를 실행하면
   이 파일을 로드해서 UI에 그래프/스케줄을 표시.
"""

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

DBT_PROJECT_DIR = Path(__file__).parent.parent
DBT_EXE = r"C:\Users\sksms\AppData\Local\Programs\Python\Python313\Scripts\dbt.exe"
DBT_PROFILES_DIR = r"C:\Users\sksms\.dbt"


# ─────────────────────────────────────────────────────────────
# Job: 전체 파이프라인
# ─────────────────────────────────────────────────────────────
# 💡 define_asset_job은 선택한 Asset들을 의존성 순서대로 자동 실행.
#    순서를 직접 지정할 필요 없음 — Dagster가 의존성 그래프를 보고 결정.
jaffle_shop_job = define_asset_job(
    name="jaffle_shop_pipeline",
    selection=AssetSelection.all(),
    description="합성 데이터 생성 → Bronze 적재 → Silver dbt → SCD2 Snapshot",
)


# ─────────────────────────────────────────────────────────────
# Schedule: 매일 오전 9시 자동 실행
# ─────────────────────────────────────────────────────────────
# 💡 cron 표현식: "분 시 일 월 요일"
#    "0 9 * * *" = 매일 09:00
#    "0 */6 * * *" = 6시간마다
#    "0 9 * * 1" = 매주 월요일 09:00
daily_schedule = ScheduleDefinition(
    job=jaffle_shop_job,
    cron_schedule="0 9 * * *",
    name="daily_jaffle_shop",
)


# ─────────────────────────────────────────────────────────────
# Definitions: Dagster 진입점
# ─────────────────────────────────────────────────────────────
defs = Definitions(
    assets=[
        synthetic_data,
        raw_orders,
        raw_payments,
        raw_customers,
        jaffle_shop_dbt_assets,
    ],
    resources={
        # 💡 DbtCliResource: dbt 명령을 subprocess로 실행.
        #    project_dir = dbt_project.yml 위치
        #    profiles_dir = profiles.yml 위치 (기본: ~/.dbt)
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=DBT_PROFILES_DIR,
        ),
    },
    jobs=[jaffle_shop_job],
    schedules=[daily_schedule],
)
