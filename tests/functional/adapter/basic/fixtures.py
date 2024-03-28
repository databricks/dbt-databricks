from dbt.tests.adapter.basic import files

basic_seed_csv = """
id,msg
1,hello
2,goodbye
2,yo
3,anyway
"""

basic_model_sql = """
{{ config(
    materialized = 'table'
)}}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg
"""

config_materialized_table_with_format = """
  {{ config(materialized="table", file_format='delta') }}
"""

incremental_sql = config_materialized_table_with_format + files.model_incremental

incremental_not_schema_change_sql = """
{{
    config(
        materialized="incremental",
        unique_key="user_id_current_time",
        on_schema_change="sync_all_columns",
        file_format='delta'
    )
}}
select
    1 || '-' || current_timestamp as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""
