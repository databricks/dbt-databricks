{{ config(
    catalog = env_var('DBT_DATABRICKS_UC_ALTERNATIVE_CATALOG', 'hive_metastore'),
    materialized = 'table'
) }}

select * from {{ ref('seed') }}
