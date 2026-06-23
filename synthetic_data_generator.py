"""
Jaffle Shop 합성 데이터 생성기
================================
실행할 때마다 새 주문/결제/고객변경 CSV를 생성해서 S3에 업로드합니다.
Bronze COPY INTO가 새 파일만 감지해서 적재 → incremental 패턴 학습용

사전 준비:
  pip install boto3
  aws configure  (Access Key ID, Secret Access Key, Region: us-west-2)

실행:
  python synthetic_data_generator.py
  python synthetic_data_generator.py --orders 20 --customer-changes 5
"""

import csv
import random
import argparse
import boto3
from datetime import date, timedelta, datetime
from io import StringIO

# ─────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────
BUCKET          = "jaffleshopclassic-usw2"
S3_PREFIX       = "raw"
REGION          = "us-west-2"

# Bronze 현재 최댓값 (새 데이터는 여기서부터 이어짐)
START_ORDER_ID   = 100   # 기존 max: 99
START_PAYMENT_ID = 114   # 기존 max: 113
MAX_CUSTOMER_ID  = 100   # 기존 고객 수

# 주문 날짜 범위 (기존 max: 2018-04-09 → 그 이후부터)
ORDER_DATE_START = date(2018, 4, 10)
ORDER_DATE_END   = date(2018, 6, 30)

# 주문 상태 가중치
ORDER_STATUSES = [
    ("completed",       0.55),
    ("shipped",         0.20),
    ("placed",          0.10),
    ("return_pending",  0.08),
    ("returned",        0.07),
]

# 결제 수단 가중치
PAYMENT_METHODS = [
    ("credit_card",   0.55),
    ("bank_transfer", 0.25),
    ("coupon",        0.12),
    ("gift_card",     0.08),
]

# SCD2 테스트용 가짜 성씨
FAKE_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones",
    "Garcia", "Miller", "Davis", "Wilson", "Moore",
    "Taylor", "Anderson", "Thomas", "Jackson", "White",
]

# ─────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────

def weighted_choice(choices):
    values, weights = zip(*choices)
    r = random.random()
    cumulative = 0.0
    for v, w in zip(values, weights):
        cumulative += w
        if r < cumulative:
            return v
    return values[-1]


def random_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def to_csv_string(fieldnames, rows):
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────
# 데이터 생성
# ─────────────────────────────────────────────────────────

def generate_orders(n, start_id):
    orders = []
    for i in range(n):
        orders.append({
            "id":         start_id + i,
            "user_id":    random.randint(1, MAX_CUSTOMER_ID),
            "order_date": random_date(ORDER_DATE_START, ORDER_DATE_END).isoformat(),
            "status":     weighted_choice(ORDER_STATUSES),
        })
    print(f"  [OK] 주문 {n}건 생성 (ID {start_id} ~ {start_id + n - 1})")
    return orders


def generate_payments(orders, start_id):
    payments = []
    pid = start_id
    for order in orders:
        if order["status"] == "returned" and random.random() < 0.3:
            continue
        n_payments = 2 if random.random() < 0.20 else 1
        total_cents = random.randint(10000, 50000)
        for j in range(n_payments):
            if n_payments == 2:
                amount = total_cents // 2 if j == 0 else total_cents - (total_cents // 2)
            else:
                amount = total_cents
            payments.append({
                "id":             pid,
                "order_id":       order["id"],
                "payment_method": weighted_choice(PAYMENT_METHODS),
                "amount":         amount,
            })
            pid += 1
    print(f"  [OK] 결제 {len(payments)}건 생성 (ID {start_id} ~ {pid - 1})")
    return payments


def generate_customer_updates(n):
    sample_first_names = [
        "Michael", "Shawn", "Kathleen", "Jimmy", "Katherine",
        "Sarah", "Martin", "Frank", "Jennifer", "Henry",
        "Fred", "Amy", "Steve", "Teresa", "Amanda",
        "Kimberly", "Johnny", "Virginia", "Anna", "Patrick",
    ]
    chosen_ids = random.sample(range(1, MAX_CUSTOMER_ID + 1), min(n, MAX_CUSTOMER_ID))
    updates = []
    for cid in chosen_ids:
        updates.append({
            "id":         cid,
            "first_name": sample_first_names[cid % len(sample_first_names)],
            "last_name":  random.choice(FAKE_LAST_NAMES) + ".",
        })
    print(f"  [OK] 고객 이름 변경 {len(updates)}건 (customer_id: {sorted(chosen_ids)})")
    print(f"    → dbt snapshot 실행 시 SCD2 이력 {len(updates)}건 생성 예정")
    return updates


# ─────────────────────────────────────────────────────────
# S3 업로드
# ─────────────────────────────────────────────────────────

def upload_to_s3(csv_content, s3_key, s3_client):
    s3_client.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"  [OK] s3://{BUCKET}/{s3_key}")


# ─────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────

def main(n_orders, n_customer_changes):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\n{'='*55}")
    print(f" Jaffle Shop 합성 데이터 생성기  [{ts}]")
    print(f"{'='*55}")

    print("\n[1/3] 데이터 생성 중...")
    orders           = generate_orders(n_orders, START_ORDER_ID)
    payments         = generate_payments(orders, START_PAYMENT_ID)
    customer_updates = generate_customer_updates(n_customer_changes) if n_customer_changes > 0 else []

    print("\n[2/3] S3 업로드 중...")
    s3 = boto3.client("s3", region_name=REGION)

    upload_to_s3(
        to_csv_string(["id", "user_id", "order_date", "status"], orders),
        f"{S3_PREFIX}/orders/raw_orders_{ts}.csv", s3
    )
    upload_to_s3(
        to_csv_string(["id", "order_id", "payment_method", "amount"], payments),
        f"{S3_PREFIX}/payments/raw_payments_{ts}.csv", s3
    )
    if customer_updates:
        upload_to_s3(
            to_csv_string(["id", "first_name", "last_name"], customer_updates),
            f"{S3_PREFIX}/customers/raw_customers_{ts}.csv", s3
        )

    print("\n[3/3] 완료! 다음 단계:")
    print("  1. Databricks Notebook에서 bronze_incremental.sql 실행")
    print("  2. dbt run")
    print("  3. dbt snapshot")
    print()
    print(f"{'='*55}")
    print(f"  주문:       {len(orders):>5}건")
    print(f"  결제:       {len(payments):>5}건")
    print(f"  고객 변경:  {len(customer_updates):>5}건")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders",           type=int, default=15)
    parser.add_argument("--customer-changes", type=int, default=3)
    args = parser.parse_args()
    main(args.orders, args.customer_changes)
