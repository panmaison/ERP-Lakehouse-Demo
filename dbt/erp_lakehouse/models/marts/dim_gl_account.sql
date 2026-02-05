{{ config(materialized='table') }}
select * from {{ ref('stg_gl_account') }}
