/*
model name: fact_daily_products_sold
description: Gives daily snapshot of total units sold for a product
*/
--Setting partition for faster load time (with previous day and today to reduce scanning)
{% set partitions_to_replace = [
    'current_date',
    'date_sub(current_date, interval 1 day)'
] %}

{{config(
    materialized = 'incremental',
    partition_by = { 'field': 'date', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
    partitions = partitions_to_replace
)}}

SELECT DATE(d.date_day) as date
, p.product_id
, count(oi.product_id) as total_units_sold
FROM {{ source('dbt_sandbox1','dim_date') }} d
LEFT JOIN {{ source('raw','orders') }} o ON DATE(d.date_day) = DATE(o.order_purchase_timestamp)
LEFT JOIN {{ source('raw','order_items') }} oi ON oi.order_id = o.order_id
LEFT JOIN {{ source('raw','products') }} p ON oi.product_id = p.product_id
{% if is_incremental() %}
  where DATE(d.date_day) >= ( select max(DATE(d.date_day)) from {{ this }} )
{% endif %}
GROUP BY d.date_day, p.product_id
-- ORDER BY total_units_sold DESC

