-- Gold: 월별 주문 집계 — 상태 분포, 반품률, 신규 고객 수
WITH monthly_orders AS (
    SELECT
        DATE_TRUNC('MONTH', order_date) AS month,
        COUNT(*)                                                        AS total_orders,
        COUNT(CASE WHEN status = 'completed'      THEN 1 END)          AS completed_orders,
        COUNT(CASE WHEN status = 'shipped'        THEN 1 END)          AS shipped_orders,
        COUNT(CASE WHEN status = 'placed'         THEN 1 END)          AS placed_orders,
        COUNT(CASE WHEN status = 'return_pending' THEN 1 END)          AS return_pending_orders,
        COUNT(CASE WHEN status = 'returned'       THEN 1 END)          AS returned_orders
    FROM {{ ref('stg_orders') }}
    GROUP BY DATE_TRUNC('MONTH', order_date)
),

-- 고객별 첫 주문월 → 신규 고객 집계
first_order AS (
    SELECT
        customer_id,
        DATE_TRUNC('MONTH', MIN(order_date)) AS first_order_month
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),

new_customers AS (
    SELECT first_order_month AS month, COUNT(*) AS new_customers
    FROM first_order
    GROUP BY first_order_month
)

SELECT
    mo.month,
    mo.total_orders,
    mo.completed_orders,
    mo.shipped_orders,
    mo.placed_orders,
    mo.return_pending_orders,
    mo.returned_orders,
    ROUND(mo.returned_orders * 100.0 / NULLIF(mo.total_orders, 0), 2) AS return_rate_pct,
    COALESCE(nc.new_customers, 0)                                      AS new_customers
FROM monthly_orders mo
LEFT JOIN new_customers nc ON mo.month = nc.month
-- W5: ORDER BY 제거
