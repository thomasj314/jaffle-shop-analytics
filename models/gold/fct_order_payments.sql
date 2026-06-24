-- MetricFlow semantic layer용 조인 팩트 테이블
-- payments에 order_date(시간 차원)를 붙여 MetricFlow agg_time_dimension 요건 충족
SELECT
    p.payment_id,
    p.order_id,
    p.payment_method,
    p.amount,
    o.customer_id,
    o.order_date,
    o.status
FROM {{ ref('stg_payments') }} p
JOIN {{ ref('stg_orders') }}   o ON p.order_id = o.order_id
