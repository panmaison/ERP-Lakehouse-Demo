{{ config(materialized='table') }}
select * from {{ source('bronze','bronze_gl_entry') }}
