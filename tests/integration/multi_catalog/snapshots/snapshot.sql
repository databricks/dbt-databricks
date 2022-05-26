{% snapshot my_snapshot %}

    {{
        config(
          check_cols=["name", "date"],
          unique_key="id",
          strategy="check",
          target_schema=schema,
          target_database=env_var('DBT_DATABRICKS_UC_ALTERNATIVE_CATALOG', 'hive_metastore'),
        )
    }}

    select * from {{ ref('seed') }}

{% endsnapshot %}
