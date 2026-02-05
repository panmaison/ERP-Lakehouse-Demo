{{ config(materialized='table') }}
select * from {{ source('bronze','bronze_customer_ledger_entry') }}
