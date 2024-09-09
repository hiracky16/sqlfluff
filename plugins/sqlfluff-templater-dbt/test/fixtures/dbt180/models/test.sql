{{
    config(materialized='table')
}}
with tmp as (
select * from {{ ref('test') }}
)
select count(*) from tmp
