
with tmp as (
select * from "postgres"."dbt_alice"."test"
)
select count(*) from tmp