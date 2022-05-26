{{ config(
    materialized = 'table'
) }}

select * from {{ ref('alternative_catalog') }}
