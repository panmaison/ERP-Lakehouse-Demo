{{ config(materialized='table') }}

select
  customer_id,
  customer_name,
  country,
  created_at
from {{ ref('stg_customer') }}