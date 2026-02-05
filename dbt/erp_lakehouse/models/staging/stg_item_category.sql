{{ config(materialized='table') }}
select * from {{ source('bronze','bronze_item_category') }}
