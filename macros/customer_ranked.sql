/*
macro_name: customer_ranked
description: Macro function to rank puchases done by customer based on order_purchase_timestamp
usage: customer_ranked(sort)
params: sort
params values: 'ASC' or 'DESC'

*/
{% macro customer_ranked(sort) %}
    (
        SELECT c.customer_unique_id
        ,o.order_purchase_timestamp AS cust_n_order
        , RANK() OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp {{ sort }}) AS cust_pur_rank
        FROM {{ source('raw','orders') }} o
        JOIN {{ source('raw','customers') }}  c on o.customer_id =c.customer_id
    )
{% endmacro %}
