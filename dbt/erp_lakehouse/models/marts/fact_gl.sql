{{ config(materialized='table') }}

with gl as (
  select
    *,
    cast(date_format(posting_date, 'yyyyMMdd') as int) as date_key
  from {{ ref('stg_gl_entry') }}
)

select
  entry_id,
  posting_date,
  date_key,
  gl_account_no,
  amount

from gl
