-- R2: amount cents → USD (/100.0)
SELECT
    CAST(id       AS BIGINT)             AS payment_id,
    CAST(order_id AS BIGINT)             AS order_id,
    payment_method,
    CAST(amount AS DECIMAL(10,2)) / 100.0 AS amount
FROM {{ source('bronze', 'raw_payments') }}
WHERE id IS NOT NULL
  AND CAST(amount AS DECIMAL(10,2)) >= 0
