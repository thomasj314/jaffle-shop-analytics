-- R2: amount cents → USD, 음수 필터링
-- Bronze에 같은 payment_id가 여러 파일에 걸쳐 존재할 수 있으므로
-- _ingested_at 기준 최신 1건만 선택
WITH ranked AS (
    SELECT
        CAST(id       AS BIGINT)              AS payment_id,
        CAST(order_id AS BIGINT)              AS order_id,
        payment_method,
        CAST(amount AS DECIMAL(10,2)) / 100.0 AS amount,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(id AS BIGINT)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'raw_payments') }}
    WHERE id IS NOT NULL
      AND CAST(amount AS DECIMAL(10,2)) >= 0
)
SELECT
    payment_id,
    order_id,
    payment_method,
    amount
FROM ranked
WHERE rn = 1
