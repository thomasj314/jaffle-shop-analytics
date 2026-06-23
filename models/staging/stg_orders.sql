-- R2: order_date STRING → DATE (TRY_CAST로 안전하게)
-- status enum 필터링
SELECT
    CAST(id      AS BIGINT)       AS order_id,
    CAST(user_id AS BIGINT)       AS customer_id,
    TRY_CAST(order_date AS DATE)  AS order_date,
    status,
    _ingested_at,
    _source_file
FROM {{ source('bronze', 'raw_orders') }}
WHERE id IS NOT NULL
  AND status IN ('placed','shipped','completed','return_pending','returned')
