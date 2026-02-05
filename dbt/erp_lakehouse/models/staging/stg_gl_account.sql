{{ config(materialized='table') }}
select * from {{ source('bronze','bronze_gl_account') }}
