{{ config(materialized='table') }}

select
  item_id,
  item_name,
  item_category,
  unit_cost
from {{ ref('stg_item') }}
