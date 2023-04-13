{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'replace_where',
    incremental_predicates = "id >= 2"
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
