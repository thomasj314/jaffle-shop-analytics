#!/bin/bash
# dbt docs 서비스 최초 설치 스크립트
# GitHub Actions 첫 배포 시 자동 실행됨

set -e

cd ~/jaffle-shop-analytics

# 1. docs 생성
/home/ubuntu/.local/bin/dbt docs generate

# 2. systemd 서비스 설치 (없을 때만)
if [ ! -f /etc/systemd/system/dbt-docs.service ]; then
    sudo cp deploy/dbt-docs.service /etc/systemd/system/dbt-docs.service
    sudo systemctl daemon-reload
    sudo systemctl enable dbt-docs
    echo "dbt-docs service installed"
fi

# 3. 서비스 시작/재시작
sudo systemctl restart dbt-docs
echo "dbt-docs started on port 8080"
