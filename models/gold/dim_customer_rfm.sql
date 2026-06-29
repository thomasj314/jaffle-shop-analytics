-- Gold: RFM 세그먼테이션
-- R: 마지막 구매 이후 경과일 (반품 제외, returned 아닌 주문 기준 — 구매 의도 포함)
-- F: completed 주문 횟수 (확정 구매 횟수)
-- M: completed 주문 결제 금액 합계 (확정 매출 기준, revenue_net과 동일)
-- NTILE(3): 1~3 점수 (데이터 분포가 좁을 때 5구간은 허수 구분 발생)
WITH ref_date AS (
    SELECT MAX(order_date) AS max_date
    FROM {{ ref('stg_orders') }}
),

recency_base AS (
    -- R: 반품 제외한 마지막 주문일 기준
    SELECT
        customer_id,
        MAX(order_date) AS last_order_date
    FROM {{ ref('stg_orders') }}
    WHERE status != 'returned'
    GROUP BY customer_id
),

fm_base AS (
    -- F, M: completed 주문만
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(p.amount)              AS monetary
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
    WHERE o.status = 'completed'
    GROUP BY o.customer_id
),

customer_metrics AS (
    SELECT
        r.customer_id,
        DATEDIFF(rd.max_date, r.last_order_date) AS recency_days,
        f.frequency,
        ROUND(f.monetary, 2)                     AS monetary
    FROM recency_base r
    JOIN fm_base f ON r.customer_id = f.customer_id
    CROSS JOIN ref_date rd
),

rfm_scores AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        -- R: 낮을수록 최근 → DESC 정렬 시 3점이 최근
        NTILE(3) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(3) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(3) OVER (ORDER BY monetary ASC)      AS m_score
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
        WHEN r_score = 3 AND f_score = 3                  THEN 'Champions'
        WHEN r_score >= 2 AND f_score >= 2                THEN 'Loyal Customers'
        WHEN r_score = 3 AND f_score = 1                  THEN 'New Customers'
        WHEN r_score = 1 AND f_score >= 2                 THEN 'At Risk'
        WHEN r_score = 1 AND f_score = 1                  THEN 'Lost'
        ELSE                                                   'Potential Loyalists'
    END AS rfm_segment
FROM rfm_scores
