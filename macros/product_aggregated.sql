/*
macro_name: product_aggregated
description: Macro function to get aggregated information for product
usage: product_aggregated()
params: N/A
*/
{% macro product_aggregated() %}
    (
        SELECT p.product_id
,product_length_cm*product_height_cm*product_width_cm AS product_volume
,count(oi.product_id) AS total_units_sold
,SUM(oi.price+ oi.freight_value) AS total_revenue
FROM {{ source('raw','products') }} p
JOIN {{ source('raw','order_items') }} oi ON oi.product_id = p.product_id
GROUP BY product_id
,product_length_cm*product_height_cm*product_width_cm
)
{% endmacro %}
