-- R2: Bronze의 STRING 타입을 올바른 타입으로 캐스팅
-- 합성 데이터 생성기가 같은 customer_id의 변경 버전을 새 파일로 올릴 수 있으므로
-- _ingested_at 기준 최신 1건만 선택 → dbt snapshot이 변경을 정확히 감지
WITH ranked AS (
    SELECT
        CAST(id AS BIGINT) AS customer_id,
        first_name,
        last_name,
        _ingested_at,
        _source_file,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(id AS BIGINT)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'raw_customers') }}
    WHERE id IS NOT NULL
)
SELECT
    customer_id,
    first_name,
    last_name,
    _ingested_at,
    _source_file
FROM ranked
WHERE rn = 1
