{{ config(materialized='table') }}

with bounds as (
  select min(dt) as min_date, max(dt) as max_date
  from (
    select min(order_date) as dt from {{ ref('stg_sales_header') }}
    union all
    select min(posting_date) as dt from {{ ref('stg_item_ledger_entry') }}
    union all
    select min(posting_date) as dt from {{ ref('stg_gl_entry') }}

    union all

    select max(order_date) as dt from {{ ref('stg_sales_header') }}
    union all
    select max(posting_date) as dt from {{ ref('stg_item_ledger_entry') }}
    union all
    select max(posting_date) as dt from {{ ref('stg_gl_entry') }}
  ) t
),

date_spine as (
  select explode(sequence(min_date, max_date, interval 1 day)) as date_day
  from bounds
)

select
  date_day as date,
  cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
  year(date_day) as year,
  quarter(date_day) as quarter,
  month(date_day) as month,
  date_format(date_day, 'MMMM') as month_name,
  day(date_day) as day,
  dayofweek(date_day) as day_of_week,        -- 1=Sunday ... 7=Saturday (Databricks)
  date_format(date_day, 'EEEE') as day_name,
  weekofyear(date_day) as week_of_year,
  case when dayofweek(date_day) in (1,7) then true else false end as is_weekend
from date_spine
order by date_day
