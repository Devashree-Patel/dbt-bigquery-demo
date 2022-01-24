/*
model name: dim_products
description: Shows the total revenue and units sold for a product along with the product ranking of 'Top 10' or 'Not Top 10'
*/

-- Query with using product_aggregated() macros

WITH prod_rank AS(SELECT product_id
,RANK() OVER (ORDER BY total_units_sold DESC) AS product_rank
FROM  {{ product_aggregated() }}
)
SELECT p.product_id
,p.product_volume
,p.total_units_sold
,p.total_revenue
,CASE WHEN (pr.product_rank<=10) THEN 'Top 10' ELSE 'Not Top 10'END AS is_top_10
FROM {{ product_aggregated() }} p
JOIN prod_rank pr ON p.product_id = pr.product_id
ORDER BY is_top_10 DESC


-- Query without using macros

/*
WITH product_details AS (
SELECT p.product_id
,product_length_cm*product_height_cm*product_width_cm AS product_volume
,count(oi.product_id) AS total_units_sold
,SUM(oi.price+ oi.freight_value) AS total_revenue
FROM {{ source('raw','products') }} p
JOIN {{ source('raw','order_items') }} oi ON oi.product_id = p.product_id0
GROUP BY product_id
,product_length_cm*product_height_cm*product_width_cm
)
,prod_rank AS(SELECT product_id
,RANK() OVER (ORDER BY total_units_sold DESC) AS product_rank
FROM  product_details
)
SELECT p.product_id
,p.product_volume
,p.total_units_sold
,p.total_revenue
,CASE WHEN (pr.product_rank<=10) THEN 'Top 10' ELSE 'Not Top 10'END AS is_top_10
FROM product_details p
JOIN prod_rank pr ON p.product_id = pr.product_id
ORDER BY is_top_10 DESC
*/
