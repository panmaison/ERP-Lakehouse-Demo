{{ config(materialized='table') }}
select * from {{ source('bronze','bronze_vendor_ledger_entry') }}
