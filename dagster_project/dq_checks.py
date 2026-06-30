"""
Business DQ Checks — Dagster Asset-based Data Quality
======================================================
Gold 테이블 대상 비즈니스 이상 감지.
파이프라인(assets.py)과 독립적으로 스케줄 실행.

체크 항목:
  1. 일별 매출 전일 대비 50% 이상 급변
  2. 반품률 20% 초과
  3. Gold 테이블 row count = 0
  4. AOV 이상값 ($0 이하 또는 $10,000 초과)
  5. 신규 고객 수 갑자기 0
"""

import os
from dagster import asset, AssetExecutionContext, Output, define_asset_job, ScheduleDefinition


# ─────────────────────────────────────────────────────────────
# Databricks SQL 실행 헬퍼
# ─────────────────────────────────────────────────────────────
def run_sql(query: str):
    from databricks import sql
    with sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            try:
                return cursor.fetchall()
            except Exception:
                return []


def check(context: AssetExecutionContext, name: str, passed: bool, detail: str):
    status = "✅ PASS" if passed else "❌ FAIL"
    context.log.info(f"[{name}] {status} — {detail}")
    return {"check": name, "passed": passed, "detail": detail}


# ═════════════════════════════════════════════════════════════
# DQ Asset 1 — Gold 테이블 Row Count 체크
# ═════════════════════════════════════════════════════════════
@asset(
    group_name="dq_business",
    description="Gold 테이블 6개 모두 데이터가 있는지 확인 (row count > 0)",
)
def dq_gold_row_counts(context: AssetExecutionContext):
    tables = [
        "fct_daily_revenue",
        "fct_monthly_orders",
        "fct_payment_method_mix",
        "dim_customer_ltv",
        "dim_customer_rfm",
        "fct_cohort_retention",
    ]
    results = []
    all_passed = True

    for table in tables:
        rows = run_sql(f"SELECT COUNT(*) AS cnt FROM jaffle_shop.gold.{table}")
        cnt = rows[0][0] if rows else 0
        passed = cnt > 0
        if not passed:
            all_passed = False
        results.append(check(context, f"row_count.{table}", passed, f"{cnt}행"))

    return Output(
        results,
        metadata={
            "total_checks": len(results),
            "passed": sum(1 for r in results if r["passed"]),
            "failed": sum(1 for r in results if not r["passed"]),
        },
    )


# ═════════════════════════════════════════════════════════════
# DQ Asset 2 — 일별 매출 급변 감지
# ═════════════════════════════════════════════════════════════
@asset(
    group_name="dq_business",
    description="최근 2일 매출 비교 — 전일 대비 50% 이상 급변 시 FAIL",
)
def dq_revenue_anomaly(context: AssetExecutionContext):
    rows = run_sql("""
        WITH recent AS (
            SELECT
                order_date,
                revenue,
                LAG(revenue) OVER (ORDER BY order_date) AS prev_revenue
            FROM jaffle_shop.gold.fct_daily_revenue
            ORDER BY order_date DESC
            LIMIT 2
        )
        SELECT
            order_date,
            revenue,
            prev_revenue,
            CASE
                WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL
                ELSE ROUND(ABS(revenue - prev_revenue) / prev_revenue * 100, 1)
            END AS change_pct
        FROM recent
        WHERE prev_revenue IS NOT NULL
        LIMIT 1
    """)

    if not rows:
        return Output(
            [check(context, "revenue_anomaly", True, "데이터 부족 — 체크 스킵")],
            metadata={"result": "skipped"},
        )

    row = rows[0]
    order_date, revenue, prev_revenue, change_pct = row
    change_pct = float(change_pct) if change_pct else 0.0
    passed = change_pct < 50.0

    result = check(
        context, "revenue_anomaly", passed,
        f"{order_date}: ${float(revenue):.2f} (전일 대비 {change_pct:.1f}% 변화)"
    )
    return Output([result], metadata={"change_pct": change_pct})


# ═════════════════════════════════════════════════════════════
# DQ Asset 3 — 반품률 이상 감지
# ═════════════════════════════════════════════════════════════
@asset(
    group_name="dq_business",
    description="최근 월 반품률이 20%를 초과하면 FAIL",
)
def dq_return_rate(context: AssetExecutionContext):
    rows = run_sql("""
        SELECT month, return_rate_pct
        FROM jaffle_shop.gold.fct_monthly_orders
        ORDER BY month DESC
        LIMIT 1
    """)

    if not rows:
        return Output(
            [check(context, "return_rate", True, "데이터 없음 — 스킵")],
            metadata={"result": "skipped"},
        )

    month, rate = rows[0]
    rate = float(rate)
    passed = rate <= 20.0

    result = check(
        context, "return_rate", passed,
        f"{str(month)[:7]}: 반품률 {rate:.1f}% (기준: 20% 이하)"
    )
    return Output([result], metadata={"return_rate_pct": rate})


# ═════════════════════════════════════════════════════════════
# DQ Asset 4 — AOV 이상값 감지
# ═════════════════════════════════════════════════════════════
@asset(
    group_name="dq_business",
    description="최근 7일 AOV가 $0 이하 또는 $10,000 초과이면 FAIL",
)
def dq_aov_range(context: AssetExecutionContext):
    rows = run_sql("""
        SELECT
            MIN(aov) AS min_aov,
            MAX(aov) AS max_aov,
            AVG(aov) AS avg_aov
        FROM (
            SELECT aov
            FROM jaffle_shop.gold.fct_daily_revenue
            ORDER BY order_date DESC
            LIMIT 7
        )
    """)

    if not rows or rows[0][0] is None:
        return Output(
            [check(context, "aov_range", True, "데이터 없음 — 스킵")],
            metadata={"result": "skipped"},
        )

    min_aov, max_aov, avg_aov = [float(v) for v in rows[0]]
    passed = min_aov > 0 and max_aov < 10000

    result = check(
        context, "aov_range", passed,
        f"AOV 범위: ${min_aov:.2f} ~ ${max_aov:.2f} (평균 ${avg_aov:.2f})"
    )
    return Output([result], metadata={"min_aov": min_aov, "max_aov": max_aov, "avg_aov": avg_aov})


# ═════════════════════════════════════════════════════════════
# DQ Asset 5 — 신규 고객 수 0 감지
# ═════════════════════════════════════════════════════════════
@asset(
    group_name="dq_business",
    description="최근 월 신규 고객 수가 0이면 FAIL (데이터 파이프라인 문제 가능성)",
)
def dq_new_customers(context: AssetExecutionContext):
    rows = run_sql("""
        SELECT month, new_customers
        FROM jaffle_shop.gold.fct_monthly_orders
        ORDER BY month DESC
        LIMIT 1
    """)

    if not rows:
        return Output(
            [check(context, "new_customers", True, "데이터 없음 — 스킵")],
            metadata={"result": "skipped"},
        )

    month, new_customers = rows[0]
    new_customers = int(new_customers)
    passed = new_customers > 0

    result = check(
        context, "new_customers", passed,
        f"{str(month)[:7]}: 신규 고객 {new_customers}명"
    )
    return Output([result], metadata={"new_customers": new_customers})


# ═════════════════════════════════════════════════════════════
# Job + Schedule 정의
# ═════════════════════════════════════════════════════════════
dq_job = define_asset_job(
    name="business_dq_job",
    selection=[
        "dq_gold_row_counts",
        "dq_revenue_anomaly",
        "dq_return_rate",
        "dq_aov_range",
        "dq_new_customers",
    ],
    description="비즈니스 DQ 체크 — Gold 테이블 이상 감지",
)

# Daily 10:00 KST (UTC 01:00)
dq_schedule = ScheduleDefinition(
    name="daily_business_dq",
    job=dq_job,
    cron_schedule="0 1 * * *",  # UTC 01:00 = KST 10:00
    description="Daily 10:00 KST - Business DQ auto check",
)
