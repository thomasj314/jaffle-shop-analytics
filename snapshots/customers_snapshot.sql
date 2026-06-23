{% snapshot customers_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name'],
    )
}}

-- SCD2: 이름 변경 시 이전 행 종료(dbt_valid_to), 새 행 삽입
SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}
