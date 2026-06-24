{{
  config(
    materialized = 'table',
  )
}}

-- MetricFlow time spine: MetricFlow가 시계열 집계에 사용하는 날짜 차원 테이블
-- Databricks SEQUENCE 함수로 날짜 범위 생성
SELECT
  CAST(date_day AS DATE) AS date_day
FROM (
  SELECT EXPLODE(
    SEQUENCE(
      DATE '2016-01-01',
      CURRENT_DATE(),
      INTERVAL 1 DAY
    )
  ) AS date_day
)
