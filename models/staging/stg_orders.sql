-- R2: order_date STRING → DATE, status enum 필터링
-- Bronze에 같은 order_id가 여러 파일에 걸쳐 존재할 수 있으므로
-- _ingested_at 기준 최신 1건만 선택 (stg_customers와 동일 패턴)
WITH ranked AS (
    SELECT
        CAST(id      AS BIGINT)      AS order_id,
        CAST(user_id AS BIGINT)      AS customer_id,
        TRY_CAST(order_date AS DATE) AS order_date,
        status,
        _ingested_at,
        _source_file,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(id AS BIGINT)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'raw_orders') }}
    WHERE id IS NOT NULL
      AND status IN ('placed','shipped','completed','return_pending','returned')
)
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    _ingested_at,
    _source_file
FROM ranked
WHERE rn = 1
