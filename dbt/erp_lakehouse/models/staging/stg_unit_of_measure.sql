{{ config(materialized='table') }}
select * from {{ source('bronze','bronze_unit_of_measure') }}
