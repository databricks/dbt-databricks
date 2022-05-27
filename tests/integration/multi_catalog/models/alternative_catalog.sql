{{ config(
    catalog = env_var('DBT_DATABRICKS_UC_ALTERNATIVE_CATALOG', 'alternative')
) }}

select * from {{ ref('seed') }}
