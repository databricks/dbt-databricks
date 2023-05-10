{{config(materialized='table', persist_constraints=False)}}

select * from {{ ref('seed') }}
