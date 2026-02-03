

{% test Column_date_value_not_null(model,column_name) %}

with validation as (

select max({{ column_name }}) as date_value
from {{ model }} 

)
,b as(
select cast(count(*) as string) as cnt from validation where date_value is null
)
select cnt from b where cnt<>'0'

{% endtest %}
