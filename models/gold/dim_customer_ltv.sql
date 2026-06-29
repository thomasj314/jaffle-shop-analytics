-- Gold: 고객별 LTV + 재구매 세그먼트
-- order_count  : 전체 주문 횟수 (상태 무관) — customer_segment 기준
-- ltv, aov     : completed 주문만 (확정 매출 기준) — revenue_net / aov 지표와 동일 기준
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id)                         AS order_count,  -- 전체 주문 (segment용)
        MIN(order_date)                                  AS first_order_date,
        MAX(order_date)                                  AS last_order_date,
        DATEDIFF(MAX(order_date), MIN(order_date))       AS tenure_days
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),

customer_revenue AS (
    SELECT
        o.customer_id,
        SUM(p.amount)              AS ltv,
        COUNT(DISTINCT o.order_id) AS completed_order_count  -- AOV 분모
    FROM {{ ref('stg_payments') }} p
    JOIN {{ ref('stg_orders') }} o ON p.order_id = o.order_id
    WHERE o.status = 'completed'
    GROUP BY o.customer_id
)

SELECT
    co.customer_id,
    co.order_count,
    co.first_order_date,
    co.last_order_date,
    co.tenure_days,
    ROUND(cr.ltv, 2)                                          AS ltv,
    ROUND(cr.ltv / NULLIF(cr.completed_order_count, 0), 2)   AS aov,
    CASE
        WHEN co.order_count >= 5 THEN 'High Value'
        WHEN co.order_count >= 2 THEN 'Repeat'
        ELSE 'One-time'
    END                                             AS customer_segment
FROM customer_orders co
JOIN customer_revenue cr ON co.customer_id = cr.customer_id
