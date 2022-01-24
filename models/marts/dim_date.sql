/*
model_name: dim_date
Description: dim_date model is used to create date dimension. This dimension will be instrumental in creating 'fact_daily_products_sold' as we need a record for every day irrespective of sale of a product for
*/

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2015-01-01' as date)",
    end_date="cast('2023-01-01' as date)"
   )
}}