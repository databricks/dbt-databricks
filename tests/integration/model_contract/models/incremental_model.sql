{{config(materialized='incremental', on_schema_change='append_new_columns')}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
  where date > (select max(date) from {{ this }})
{% endif %}
