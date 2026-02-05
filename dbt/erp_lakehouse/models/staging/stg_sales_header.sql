{{ config(materialized='table') }}

select
  sales_order_id,
  customer_id,
  order_date,
  status
from {{ source('bronze','bronze_sales_header') }}
