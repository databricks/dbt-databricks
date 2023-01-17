{{ config(
    materialized = 'view',
) }}

select * from {{ ref('base') }}
