# Jaffle Shop Analytics Pipeline

Jaffle Shop 이커머스 데이터를 S3에서 Databricks로 수집하고,
dbt + Dagster로 메달리온 아키텍처(Bronze → Silver → Gold)를 구축하는 데이터 파이프라인.

## 기술 스택

| 구성요소 | 역할 |
|---|---|
| AWS S3 | 원본 CSV Landing Zone |
| Databricks (us-west-2) | Bronze 적재 + Silver/Gold 처리 |
| dbt-databricks | Silver/Gold 변환, 테스트, SCD2 Snapshot |
| Dagster | 전체 파이프라인 오케스트레이션 |

## 파이프라인 흐름

```
synthetic_data_generator.py  →  S3 (raw/)
        ↓
Bronze COPY INTO  →  bronze.raw_orders / raw_payments / raw_customers
        ↓
dbt run  →  silver.stg_* (view) / silver.orders / silver.payments (table)
        ↓
dbt snapshot  →  silver.customers_snapshot (SCD2)
```

## 로컬 셋업

### 1. 필수 패키지 설치
```bash
pip install dagster dagster-webserver dagster-dbt dbt-databricks databricks-sql-connector boto3 python-dotenv
```

### 2. 환경변수 설정
`.env` 파일을 프로젝트 루트에 생성 (팀 리드에게 값 요청):
```
DATABRICKS_HOST=...
DATABRICKS_HTTP_PATH=...
DATABRICKS_TOKEN=...
```

AWS 인증은 `aws configure`로 설정.

### 3. dbt manifest 생성
```bash
dbt parse
```

### 4. Dagster UI 실행
```bash
dagster dev -m dagster_project.definitions
# → http://localhost:3000
```

## 주요 명령어

| 목적 | 명령어 |
|---|---|
| 합성 데이터 생성 + S3 업로드 | `python synthetic_data_generator.py` |
| Silver 모델 갱신 | `dbt run` |
| SCD2 이력 갱신 | `dbt snapshot` |
| 전체 파이프라인 (Dagster UI) | Materialize all |

## 디렉토리 구조

```
jaffle_shop/
  models/
    staging/          # Bronze → stg_* (view)
    silver/           # stg_* → silver 테이블
  snapshots/          # customers_snapshot (SCD2)
  macros/             # generate_schema_name
  dagster_project/    # Dagster assets + definitions
  synthetic_data_generator.py
  dbt_project.yml
  setup.py
```

## 트러블슈팅

자세한 내용은 팀 내 `jaffle_shop_handover.md` 및 `databricks_aws_setup_guide.md` 참고.

자주 발생하는 문제:
- **Python PATH 문제 (Windows)**: `python` 대신 전체 경로 사용
  `C:\Users\...\Python313\python.exe`
- **dbt 401 Unauthorized**: Databricks 토큰 scope → Other APIs + sql
- **silver_silver 스키마 중복**: Silver 모델에 `+schema: silver` 제거
