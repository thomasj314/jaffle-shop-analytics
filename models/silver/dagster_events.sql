{{
  config(
    materialized = 'incremental',
    unique_key   = ['log_time', 'service', 'pid'],
    incremental_strategy = 'merge'
  )
}}

/*
  Silver: dagster_events
  ----------------------
  bronze.dagster_log_events를 파싱해서 구조화된 이벤트 테이블로 변환.

  주요 변환:
  1. ANSI 컬러 코드 제거
  2. Dagster 내장 로그 포맷 파싱
     → "2026-06-30 06:41:08 +0000 - logger - LEVEL - content"
  3. 이벤트 타입 분류
  4. traceback/스택트레이스 라인 제거
*/

WITH raw AS (
  SELECT *
  FROM {{ source('dagster_bronze', 'dagster_log_events') }}
  {% if is_incremental() %}
    WHERE ingested_at > (
      SELECT COALESCE(MAX(ingested_at_us), 0) FROM {{ this }}
    )
  {% endif %}
),

cleaned AS (
  SELECT
    log_time,
    service,
    level AS journal_level,
    host,
    pid,
    ingested_at AS ingested_at_us,
    -- ANSI 컬러 코드 제거
    regexp_replace(message, '\x1b\\[[0-9;]*m', '') AS clean_message
  FROM raw
),

parsed AS (
  SELECT
    log_time,
    service,
    journal_level,
    host,
    pid,
    ingested_at_us,
    clean_message,

    -- Dagster 내장 포맷 여부: "YYYY-MM-DD HH:MM:SS +0000 - logger - LEVEL - content"
    CASE
      WHEN clean_message RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \\+[0-9]{4} - .+ - (INFO|WARNING|ERROR|DEBUG|CRITICAL) - '
      THEN regexp_extract(clean_message, '^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})', 1)
      ELSE NULL
    END AS msg_timestamp_str,

    CASE
      WHEN clean_message RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \\+[0-9]{4} - .+ - (INFO|WARNING|ERROR|DEBUG|CRITICAL) - '
      THEN trim(regexp_extract(clean_message, '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \\+[0-9]{4} - ([^-]+) -', 1))
      ELSE service
    END AS logger,

    CASE
      WHEN clean_message RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \\+[0-9]{4} - .+ - (INFO|WARNING|ERROR|DEBUG|CRITICAL) - '
      THEN regexp_extract(clean_message, ' - (INFO|WARNING|ERROR|DEBUG|CRITICAL) - ', 1)
      ELSE journal_level
    END AS effective_level,

    CASE
      WHEN clean_message RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \\+[0-9]{4} - .+ - (INFO|WARNING|ERROR|DEBUG|CRITICAL) - '
      THEN regexp_extract(clean_message, ' - (?:INFO|WARNING|ERROR|DEBUG|CRITICAL) - (.+)$', 1)
      ELSE clean_message
    END AS content,

    -- traceback / 스택트레이스 라인 여부 (노이즈 제거용)
    CASE
      WHEN clean_message RLIKE '^\\s*(File "|Traceback |raise |    )'
        OR clean_message RLIKE '^[a-zA-Z_.]+Error:'
        OR clean_message RLIKE '^[a-zA-Z_.]+Exception:'
      THEN TRUE
      ELSE FALSE
    END AS is_traceback

  FROM cleaned
),

classified AS (
  SELECT
    *,
    CASE
      WHEN content LIKE '%Serving dagster-webserver%'        THEN 'webserver_start'
      WHEN content LIKE '%Started Dagster code server%'      THEN 'code_server_start'
      WHEN content LIKE '%Starting Dagster code server%'     THEN 'code_server_starting'
      WHEN content LIKE '%Instance is configured with%'      THEN 'daemon_start'
      WHEN content LIKE '%No heartbeat received%'            THEN 'heartbeat_timeout'
      WHEN content LIKE '%shutting down%'
        OR content LIKE '%Shutting down%'                    THEN 'service_stop'
      WHEN content LIKE '%DAGSTER_HOME is not set%'          THEN 'config_error'
      WHEN content LIKE '%dbt run%'
        OR content LIKE '%Running dbt%'                      THEN 'dbt_run'
      WHEN content LIKE '%Completed with 0 error%'           THEN 'dbt_success'
      WHEN content LIKE '%Completed with%error%'             THEN 'dbt_failure'
      WHEN content LIKE '%Starting a run%'
        OR content LIKE '%Launching run%'                    THEN 'job_start'
      WHEN content LIKE '%Finished run%'
        OR content LIKE '%Run finished%'                     THEN 'job_finish'
      WHEN effective_level IN ('ERROR', 'CRITICAL')          THEN 'error'
      WHEN effective_level = 'WARNING'                       THEN 'warning'
      ELSE 'info'
    END AS event_type

  FROM parsed
)

SELECT
  FROM_UNIXTIME(log_time / 1000000)     AS log_time,
  service,
  logger,
  effective_level                        AS level,
  event_type,
  content,
  is_traceback,
  host,
  CAST(pid AS INT)                       AS pid,
  ingested_at_us,
  FROM_UNIXTIME(ingested_at_us / 1000000) AS ingested_at

FROM classified
WHERE NOT is_traceback   -- 스택트레이스 노이즈 제외
