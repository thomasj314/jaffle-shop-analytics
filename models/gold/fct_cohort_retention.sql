-- Gold: 월별 코호트 잔존율
-- cohort_month: 고객이 처음 주문한 달
-- months_since_first: 코호트 기준 경과 개월 수
-- retention_rate: 해당 월에 재구매한 코호트 비율
WITH first_order AS (
    SELECT
        customer_id,
        DATE_TRUNC('MONTH', MIN(order_date)) AS cohort_month
    FROM {{ ref('stg_orders') }}
    WHERE status != 'returned'  -- W2: 반품만 한 고객은 코호트에서 제외
    GROUP BY customer_id
),

-- 고객이 주문한 모든 월 (중복 제거)
customer_activity AS (
    SELECT DISTINCT
        customer_id,
        DATE_TRUNC('MONTH', order_date) AS activity_month
    FROM {{ ref('stg_orders') }}
    WHERE status != 'returned'
),

cohort_activity AS (
    SELECT
        f.cohort_month,
        ca.activity_month,
        COUNT(DISTINCT ca.customer_id) AS active_customers,
        -- MONTHS_BETWEEN 대신 직접 계산 (Databricks SQL 호환)
        CAST(
            (YEAR(ca.activity_month) - YEAR(f.cohort_month)) * 12
            + (MONTH(ca.activity_month) - MONTH(f.cohort_month))
        AS INT) AS months_since_first
    FROM first_order f
    JOIN customer_activity ca ON f.customer_id = ca.customer_id
    GROUP BY f.cohort_month, ca.activity_month
),

cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
    FROM first_order
    GROUP BY cohort_month
)

SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.months_since_first,
    ca.active_customers,
    ROUND(ca.active_customers * 100.0 / cs.cohort_size, 2) AS retention_rate_pct
FROM cohort_activity ca
JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
-- W5: ORDER BY 제거
