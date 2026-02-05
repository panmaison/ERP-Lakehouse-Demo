{{ config(materialized='table') }}

with ile as (
  select
    entry_id,
    item_id,
    posting_date,
    location_code,
    quantity,
    cast(date_format(posting_date, 'yyyyMMdd') as int) as date_key
  from {{ ref('stg_item_ledger_entry') }}
),

ve as (
  select
    entry_id,
    cost_amount
  from {{ ref('stg_value_entry') }}
)

select
  ile.entry_id,
  ile.item_id,
  ile.posting_date,
  ile.date_key,
  ile.location_code,
  ile.quantity,
  ve.cost_amount
from ile
left join ve
  on ile.entry_id = ve.entry_id
