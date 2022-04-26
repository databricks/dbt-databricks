{{config(materialized='incremental')}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
  where date > (select max(date) from {{ this }})
{% endif %}
