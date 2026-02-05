{{ config(
    materialized='table',
    catalog='erp_demo',
    schema='silver',
    alias='stg_customer'
) }}

select
  customer_id,
  customer_name,
  country,
  created_at
from {{ source('bronze','bronze_customer') }}
