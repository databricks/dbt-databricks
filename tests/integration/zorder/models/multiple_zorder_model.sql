{{ config(
    materialized='incremental',
    zorder=['id', 'name']
) }}
select 1 as id, 'Joe' as name
