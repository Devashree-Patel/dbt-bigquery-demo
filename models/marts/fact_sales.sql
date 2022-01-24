/*
model_name: fact_sales
description: Provides the total delivery time (in days) for an order item and if the customer has placed a repeat order. 
*/

SELECT oi.order_item_id
,date_diff(o.order_delivered_customer_date,o.order_purchase_timestamp,day) AS total_delivery_time
,CASE WHEN cust_pur_rank = 1  THEN 'New Purchase' ELSE 'Repeat Purchase' END AS new_repeat_purchase
 FROM {{ source('raw','order_items') }}  oi
JOIN {{ source('raw','orders') }} o on oi.order_id=o.order_id
JOIN {{ source('raw','customers') }}  c on o.customer_id = c.customer_id
JOIN {{ customer_ranked('ASC') }} cr on c.customer_unique_id = cr.customer_unique_id

-- Query without macro
/*
WITH cust_rank AS (
    SELECT c.customer_unique_id
    ,o.order_purchase_timestamp
    ,o.order_id
    ,RANK() OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp ASC) AS customer_rank
     FROM {{ source('raw','orders') }} o
    JOIN {{ source('raw','customers') }}  c on o.customer_id =c.customer_id
),
repeat_pur AS(
    SELECT order_id
    ,CASE WHEN customer_rank=1  THEN 'New Purchase' ELSE 'Repeat Purchase' END AS new_repeat_purchase
    FROM cust_rank
)

SELECT oi.order_item_id
,date_diff(o.order_delivered_customer_date,o.order_purchase_timestamp,day) AS total_delivery_time
,r.new_repeat_purchase
 FROM {{ source('raw','order_items') }}  oi
JOIN {{ source('raw','orders') }} o on oi.order_id=o.order_id
JOIN repeat_pur r ON o.order_id=r.order_id
*/