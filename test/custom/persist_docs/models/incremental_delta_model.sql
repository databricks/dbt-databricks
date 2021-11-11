{{ config(materialized='incremental') }}
select 1 as id, 'Joe' as name
