{% snapshot my_snapshot %}

    {{
        config(
          check_cols=["name", "date"],
          unique_key="id",
          strategy="check",
          target_schema=schema
        )

    }}

    select * from {{ ref('seed') }}

{% endsnapshot %}
