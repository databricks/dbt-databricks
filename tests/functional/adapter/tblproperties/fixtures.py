tblproperties_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
"""

view_tblproperties_sql = """
{{ config(
    tblproperties={
      'tblproperties_to_view' : 'true'
    }
) }}

select * from {{ ref('set_tblproperties') }}
"""

seed_csv = """id,msg
1,hello
2,yo
3,anyway
"""

snapshot_sql = """
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
"""
