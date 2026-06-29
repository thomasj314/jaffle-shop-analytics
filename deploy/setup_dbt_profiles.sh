#!/bin/bash
# EC2에서 ~/.dbt/profiles.yml 생성 스크립트
# .env 파일에서 DATABRICKS_TOKEN을 읽어 profiles.yml을 생성
# 실행: bash deploy/setup_dbt_profiles.sh

set -e

PROFILES_DIR="$HOME/.dbt"
PROFILES_FILE="$PROFILES_DIR/profiles.yml"
ENV_FILE="$HOME/jaffle-shop-analytics/.env"

# .env에서 토큰 읽기
if [ -f "$ENV_FILE" ]; then
  export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

if [ -z "$DATABRICKS_TOKEN" ]; then
  echo "ERROR: DATABRICKS_TOKEN not found in $ENV_FILE"
  exit 1
fi

mkdir -p "$PROFILES_DIR"

cat > "$PROFILES_FILE" << EOF
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: databricks
      host: dbc-b79af0ed-5484.cloud.databricks.com
      http_path: /sql/1.0/warehouses/c6d4c0096051c243
      token: ${DATABRICKS_TOKEN}
      schema: dev
      catalog: jaffle_analytics
      threads: 4
    prod:
      type: databricks
      host: dbc-b79af0ed-5484.cloud.databricks.com
      http_path: /sql/1.0/warehouses/c6d4c0096051c243
      token: ${DATABRICKS_TOKEN}
      schema: prod
      catalog: jaffle_analytics
      threads: 4
EOF

echo "profiles.yml updated successfully at $PROFILES_FILE"
echo "Targets: dev, prod"
