-- Gold: 월별 결제수단별 매출 비중
-- credit_card / bank_transfer / coupon / gift_card
SELECT
    DATE_TRUNC('MONTH', o.order_date) AS month,
    p.payment_method,
    COUNT(*)        AS payment_count,
    SUM(p.amount)   AS revenue,
    ROUND(
        SUM(p.amount) * 100.0
        / SUM(SUM(p.amount)) OVER (PARTITION BY DATE_TRUNC('MONTH', o.order_date)),
        2
    )               AS revenue_share_pct
FROM {{ ref('stg_payments') }} p
JOIN {{ ref('stg_orders') }} o ON p.order_id = o.order_id
WHERE o.status NOT IN ('returned', 'return_pending')  -- W1: 반품 주문 제외
GROUP BY DATE_TRUNC('MONTH', o.order_date), p.payment_method
