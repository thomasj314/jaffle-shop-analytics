#!/bin/bash
# EC2에서 ~/.dbt/profiles.yml 생성 스크립트
# .env 파일에서 DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN을 읽어 profiles.yml을 생성
# 실행: bash deploy/setup_dbt_profiles.sh

set -e

PROFILES_DIR="$HOME/.dbt"
PROFILES_FILE="$PROFILES_DIR/profiles.yml"
ENV_FILE="$HOME/jaffle-shop-analytics/.env"

# .env에서 변수 읽기
if [ -f "$ENV_FILE" ]; then
  export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

if [ -z "$DATABRICKS_TOKEN" ]; then
  echo "ERROR: DATABRICKS_TOKEN not found in $ENV_FILE"
  exit 1
fi

# DATABRICKS_HOST에서 https:// 제거
DATABRICKS_HOST_CLEAN=$(echo "${DATABRICKS_HOST}" | sed 's|https://||')

mkdir -p "$PROFILES_DIR"

cat > "$PROFILES_FILE" << EOF
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: databricks
      host: ${DATABRICKS_HOST_CLEAN}
      http_path: ${DATABRICKS_HTTP_PATH}
      token: ${DATABRICKS_TOKEN}
      schema: dev
      catalog: jaffle_analytics
      threads: 4
    prod:
      type: databricks
      host: ${DATABRICKS_HOST_CLEAN}
      http_path: ${DATABRICKS_HTTP_PATH}
      token: ${DATABRICKS_TOKEN}
      schema: prod
      catalog: jaffle_analytics
      threads: 4
EOF

echo "profiles.yml updated successfully at $PROFILES_FILE"
echo "Targets: dev, prod"
