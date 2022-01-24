/*
model name: dim_customers
description: Gives an insight into customers purchase patterns
*/
-- Query with using product_aggregated() macros
WITH 
first_order AS (
 SELECT customer_unique_id
    ,cust_n_order AS cust_first_order
    FROM {{ customer_ranked('ASC') }}
    WHERE cust_pur_rank=1
)

,recent_order  AS(
  SELECT customer_unique_id
    ,cust_n_order AS cust_recent_order
    FROM {{ customer_ranked('DESC') }}
    WHERE cust_pur_rank=1
)

SELECT c.customer_unique_id
,f.cust_first_order AS date_of_first_order
,r.cust_recent_order AS date_of_most_recent_order
,COUNT(o.order_id) AS number_of_orders
,SUM(p.payment_value) AS total_order_value
,MAX(p.payment_value) AS value_of_most_expensive_order
FROM {{ source('raw','customers') }} c
JOIN {{ source('raw','orders') }} o ON c.customer_id = o.customer_id
JOIN {{ source('raw','payments') }} p ON o.order_id = p.order_id
JOIN first_order f ON c.customer_unique_id = f.customer_unique_id
JOIN recent_order r ON c.customer_unique_id = r.customer_unique_id
GROUP BY customer_unique_id
,f.cust_first_order 
,r.cust_recent_order
ORDER BY number_of_orders DESC


-- Query without macros

/*
WITH first_purchase_rank AS (
    SELECT c.customer_unique_id
    ,o.order_purchase_timestamp AS cust_first_order
    , RANK() OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp ASC) AS first_pur_rank
     FROM {{ source('raw','orders') }} o
      JOIN {{ source('raw','customers') }}  c on o.customer_id =c.customer_id
  
),
first_order AS (
 SELECT customer_unique_id
    ,cust_first_order
    FROM first_purchase_rank 
    WHERE first_pur_rank=1
)
,last_purchase_order AS (
    SELECT c.customer_unique_id
    ,o.order_purchase_timestamp AS cust_recent_order
    , RANK() OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp DESC)  AS last_pur_rank
     FROM {{ source('raw','orders') }} o
      JOIN {{ source('raw','customers') }} c on o.customer_id =c.customer_id 
)
,recent_order  AS(
  SELECT customer_unique_id
    ,cust_recent_order
    FROM last_purchase_order
    WHERE last_pur_rank = 1
)


SELECT c.customer_unique_id
,f.cust_first_order AS date_of_first_order
,r.cust_recent_order AS date_of_most_recent_order
,COUNT(o.order_id) AS number_of_orders
,SUM(p.payment_value) AS total_order_value
,MAX(p.payment_value) AS value_of_most_expensive_order
FROM {{ source('raw','customers') }} c
JOIN {{ source('raw','orders') }} o ON c.customer_id = o.customer_id
JOIN {{ source('raw','payments') }} p ON o.order_id = p.order_id
JOIN first_order f ON c.customer_unique_id = f.customer_unique_id
JOIN recent_order r ON c.customer_unique_id = r.customer_unique_id
GROUP BY customer_unique_id
,f.cust_first_order 
,r.cust_recent_order
ORDER BY number_of_orders DESC
*/