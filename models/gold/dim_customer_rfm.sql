-- Gold: RFM 세그먼테이션
-- R: 마지막 구매 이후 경과일 (낮을수록 좋음)
-- F: 총 주문 횟수 (높을수록 좋음)
-- M: 총 결제 금액 (높을수록 좋음)
-- NTILE(5): 1~5 점수 → 합산 → 세그먼트 분류
WITH ref_date AS (
    -- 데이터 기준일: 가장 최근 주문일 사용
    SELECT MAX(order_date) AS max_date
    FROM {{ ref('stg_orders') }}
),

customer_metrics AS (
    -- W4: subquery → CROSS JOIN으로 변경 (옵티마이저 안전)
    SELECT
        o.customer_id,
        DATEDIFF(rd.max_date, MAX(o.order_date)) AS recency_days,
        COUNT(DISTINCT o.order_id)               AS frequency,
        SUM(p.amount)                            AS monetary
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
    CROSS JOIN ref_date rd
    WHERE o.status != 'returned'
    GROUP BY o.customer_id, rd.max_date
),

rfm_scores AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        ROUND(monetary, 2) AS monetary,
        -- recency는 낮을수록 최근 → DESC로 정렬해야 5점이 최근
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
    FROM customer_metrics
)

SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score AS rfm_total,
    CASE
        WHEN r_score >= 4 AND f_score >= 4                 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3                 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2                 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3                 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2                 THEN 'Lost'
        ELSE                                                    'Potential Loyalists'
    END AS rfm_segment
FROM rfm_scores
