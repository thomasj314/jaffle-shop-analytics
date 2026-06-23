{{ config(materialized='table') }}

-- R3: 주문 + 결제 JOIN → 주문별 총 결제금액
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    COALESCE(SUM(p.amount), 0) AS amount
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
GROUP BY o.order_id, o.customer_id, o.order_date, o.status
