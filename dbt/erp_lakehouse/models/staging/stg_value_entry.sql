{{ config(materialized='table') }}
select * from {{ source('bronze','bronze_value_entry') }}
