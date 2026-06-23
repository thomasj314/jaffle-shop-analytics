-- Gold: 일별 매출 + AOV 트렌드
-- 완료된 주문(completed)만 집계
-- R2: incremental 불필요 - 소량 데이터, table 재생성이 더 단순
SELECT
    o.order_date,
    COUNT(DISTINCT o.order_id)    AS order_count,
    COUNT(DISTINCT o.customer_id) AS customer_count,
    SUM(p.amount)                 AS revenue,
    ROUND(SUM(p.amount) / COUNT(DISTINCT o.order_id), 2) AS aov
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
WHERE o.status = 'completed'
GROUP BY o.order_date
-- W5: table materialization에서 ORDER BY 불필요 (Delta table은 무시)
