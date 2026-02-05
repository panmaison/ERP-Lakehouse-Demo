{{ config(materialized='table') }}

select
  sales_order_id,
  line_no,
  item_id,
  quantity,
  unit_price
from {{ source('bronze','bronze_sales_line') }}
