{{config(materialized='table')}}

select id as pid, name as pname, date from {{ ref('seed') }}
