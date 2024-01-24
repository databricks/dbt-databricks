{{ config(
    materialized = 'streaming_table',
) }}

select id, msg as msg from stream {{ ref('view_model') }}