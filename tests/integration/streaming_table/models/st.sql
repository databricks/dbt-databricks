{{ config(
    materialized = 'streaming_table',
) }}

select id as id, msg as msg from stream {{ ref('base') }}