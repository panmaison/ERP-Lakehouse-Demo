{{ config(materialized='table') }}

select
  sh.sales_order_id,
  sh.order_date,
  cast(date_format(sh.order_date, 'yyyyMMdd') as int) as date_key,   -- ✅ 新增
  sh.status,
  sh.customer_id,
  sl.line_no,
  sl.item_id,
  sl.quantity,
  sl.unit_price,
  sl.quantity * sl.unit_price as line_amount
from {{ ref('stg_sales_header') }} sh
join {{ ref('stg_sales_line') }} sl
  on sh.sales_order_id = sl.sales_order_id

