{{ config(materialized='table') }}

SELECT
    payment_id,
    order_id,
    payment_method,
    amount
FROM {{ ref('stg_payments') }}
