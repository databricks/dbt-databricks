{{ config(
    materialized='incremental',
    zorder="name"
) }}
select 1 as id, 'Joe' as name
