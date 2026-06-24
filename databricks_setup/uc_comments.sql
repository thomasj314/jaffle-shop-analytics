-- ============================================================
-- Unity Catalog COMMENT 등록
-- Databricks Catalog Explorer + Genie AI가 이 설명을 읽어
-- 자연어 질문 → SQL 자동 생성에 활용
--
-- 실행 위치: Databricks SQL Editor (jaffle_shop catalog)
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- GOLD 테이블 설명
-- ────────────────────────────────────────────────────────────

COMMENT ON TABLE jaffle_shop.gold.fct_daily_revenue IS
  '일별 완료 주문 기준 매출 집계. order_date별 revenue, order_count, customer_count, AOV(평균 주문 금액) 포함. 일별 매출 트렌드 분석에 사용.';

COMMENT ON TABLE jaffle_shop.gold.fct_monthly_orders IS
  '월별 주문 집계. 주문 상태별 건수(completed/shipped/placed/return_pending/returned), 반품률(return_rate_pct), 신규 고객 수(new_customers) 포함.';

COMMENT ON TABLE jaffle_shop.gold.fct_payment_method_mix IS
  '월별 결제수단별 매출 비중. payment_method(credit_card/bank_transfer/coupon/gift_card)별 revenue와 revenue_share_pct 포함. 반품 주문 제외.';

COMMENT ON TABLE jaffle_shop.gold.dim_customer_ltv IS
  '고객별 생애 가치(LTV). 총 결제 금액, 주문 횟수, AOV, 첫/마지막 주문일, 고객 세그먼트(High Value/Repeat/One-time) 포함.';

COMMENT ON TABLE jaffle_shop.gold.dim_customer_rfm IS
  'RFM 세그먼테이션. 고객을 Recency(최근성)/Frequency(빈도)/Monetary(금액) 기준 1~5점 채점 후 Champions/Loyal Customers/New Customers/At Risk/Lost/Potential Loyalists로 분류.';

COMMENT ON TABLE jaffle_shop.gold.fct_cohort_retention IS
  '월별 코호트 잔존율. 첫 구매월(cohort_month) 기준 경과 개월수(months_since_first)별 재구매 고객 수와 retention_rate_pct 포함.';

-- ────────────────────────────────────────────────────────────
-- GOLD 컬럼 설명
-- ────────────────────────────────────────────────────────────

-- fct_daily_revenue
ALTER TABLE jaffle_shop.gold.fct_daily_revenue ALTER COLUMN order_date   COMMENT '주문일 (DATE)';
ALTER TABLE jaffle_shop.gold.fct_daily_revenue ALTER COLUMN order_count  COMMENT '완료 주문 건수';
ALTER TABLE jaffle_shop.gold.fct_daily_revenue ALTER COLUMN customer_count COMMENT '해당일 구매 고객 수 (중복 제거)';
ALTER TABLE jaffle_shop.gold.fct_daily_revenue ALTER COLUMN revenue      COMMENT '총 결제 금액 (USD)';
ALTER TABLE jaffle_shop.gold.fct_daily_revenue ALTER COLUMN aov          COMMENT '평균 주문 금액 = revenue / order_count (USD)';

-- fct_monthly_orders
ALTER TABLE jaffle_shop.gold.fct_monthly_orders ALTER COLUMN month               COMMENT '주문 월 (월 첫째 날 기준)';
ALTER TABLE jaffle_shop.gold.fct_monthly_orders ALTER COLUMN total_orders        COMMENT '전체 주문 건수';
ALTER TABLE jaffle_shop.gold.fct_monthly_orders ALTER COLUMN completed_orders    COMMENT '완료된 주문 건수';
ALTER TABLE jaffle_shop.gold.fct_monthly_orders ALTER COLUMN returned_orders     COMMENT '반품된 주문 건수';
ALTER TABLE jaffle_shop.gold.fct_monthly_orders ALTER COLUMN return_rate_pct     COMMENT '반품률 (%) = returned / total * 100';
ALTER TABLE jaffle_shop.gold.fct_monthly_orders ALTER COLUMN new_customers       COMMENT '해당 월 첫 구매 고객 수';

-- fct_payment_method_mix
ALTER TABLE jaffle_shop.gold.fct_payment_method_mix ALTER COLUMN month              COMMENT '주문 월';
ALTER TABLE jaffle_shop.gold.fct_payment_method_mix ALTER COLUMN payment_method     COMMENT '결제 수단 (credit_card / bank_transfer / coupon / gift_card)';
ALTER TABLE jaffle_shop.gold.fct_payment_method_mix ALTER COLUMN revenue            COMMENT '해당 결제수단 매출 합계 (USD)';
ALTER TABLE jaffle_shop.gold.fct_payment_method_mix ALTER COLUMN revenue_share_pct  COMMENT '월 전체 매출 대비 결제수단 비중 (%)';

-- dim_customer_ltv
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN customer_id       COMMENT '고객 고유 ID';
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN order_count       COMMENT '반품 제외 총 주문 횟수';
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN first_order_date  COMMENT '첫 주문일';
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN last_order_date   COMMENT '최근 주문일';
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN tenure_days       COMMENT '첫 주문 ~ 최근 주문 경과일';
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN ltv               COMMENT '생애 총 결제 금액 (USD, 반품 제외)';
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN aov               COMMENT '평균 주문 금액 (USD)';
ALTER TABLE jaffle_shop.gold.dim_customer_ltv ALTER COLUMN customer_segment  COMMENT '고객 세그먼트: High Value(5회+) / Repeat(2~4회) / One-time(1회)';

-- dim_customer_rfm
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN customer_id   COMMENT '고객 고유 ID';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN recency_days  COMMENT '마지막 구매 이후 경과일 (낮을수록 최근)';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN frequency     COMMENT '총 주문 횟수 (반품 제외)';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN monetary      COMMENT '총 결제 금액 (USD, 반품 제외)';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN r_score       COMMENT 'Recency 점수 (1~5, 5=가장 최근 구매)';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN f_score       COMMENT 'Frequency 점수 (1~5, 5=가장 자주 구매)';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN m_score       COMMENT 'Monetary 점수 (1~5, 5=가장 고액 구매)';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN rfm_total     COMMENT 'R+F+M 합산 점수 (3~15)';
ALTER TABLE jaffle_shop.gold.dim_customer_rfm ALTER COLUMN rfm_segment   COMMENT 'RFM 세그먼트: Champions / Loyal Customers / New Customers / At Risk / Lost / Potential Loyalists';

-- fct_cohort_retention
ALTER TABLE jaffle_shop.gold.fct_cohort_retention ALTER COLUMN cohort_month        COMMENT '코호트 기준월 (고객 첫 비반품 구매월)';
ALTER TABLE jaffle_shop.gold.fct_cohort_retention ALTER COLUMN cohort_size         COMMENT '코호트 전체 고객 수';
ALTER TABLE jaffle_shop.gold.fct_cohort_retention ALTER COLUMN months_since_first  COMMENT '코호트 기준 경과 개월 수 (0 = 첫 구매월)';
ALTER TABLE jaffle_shop.gold.fct_cohort_retention ALTER COLUMN active_customers    COMMENT '해당 월에 재구매한 고객 수';
ALTER TABLE jaffle_shop.gold.fct_cohort_retention ALTER COLUMN retention_rate_pct  COMMENT '잔존율 (%) = active_customers / cohort_size * 100';

-- ────────────────────────────────────────────────────────────
-- SILVER 테이블 설명 (Genie가 lineage 추적 시 참조)
-- ────────────────────────────────────────────────────────────

COMMENT ON TABLE jaffle_shop.silver.orders IS
  'Silver 정제 주문 테이블. Bronze raw_orders에서 타입 변환 + 중복 제거(ROW_NUMBER dedup)한 신뢰할 수 있는 주문 데이터.';

COMMENT ON TABLE jaffle_shop.silver.payments IS
  'Silver 정제 결제 테이블. Bronze raw_payments에서 cents→USD 변환 + 중복 제거한 신뢰할 수 있는 결제 데이터.';

COMMENT ON TABLE jaffle_shop.silver.customers_snapshot IS
  'SCD Type 2 고객 스냅샷. 이름 변경 이력 보존. dbt_valid_to IS NULL = 현재 레코드. dbt_valid_from/to로 유효 기간 조회.';
