{% snapshot my_snapshot %}

    {{
        config(
          check_cols=["msg"],
          unique_key="id",
          strategy="check",
          target_schema=schema,
          tblproperties={
            'tblproperties_to_snapshot' : 'true'
          }
        )
    }}

    select * from {{ ref('set_tblproperties') }}

{% endsnapshot %}
