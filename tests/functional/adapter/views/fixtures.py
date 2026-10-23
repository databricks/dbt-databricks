seed_csv = """id,msg
1,hello
2,goodbye
2,yo
3,anyway
"""

view_sql = """
{{ config(materialized='view') }}
select * from {{ ref('seed') }};
"""

schema_yml = """
version: 2
models:
  - name: initial_view
    description: "This is a view"
    config:
      tblproperties:
        key: value
      databricks_tags:
        tag1: value1
    columns:
      - name: id
        description: "This is the id column"
      - name: msg
"""

no_tag_schema_yml = """
version: 2
models:
  - name: initial_view
    description: "This is a view"
    config:
      tblproperties:
        key: value
    columns:
      - name: id
        description: "This is the id column"
      - name: msg
"""

two_tag_schema_yml = """
version: 2
models:
  - name: initial_view
    description: "This is a view"
    config:
      tblproperties:
        key: value
      databricks_tags:
        tag1: value1
        tag2: value2
    columns:
      - name: id
        description: "This is the id column"
      - name: msg
"""

hive_schema_yml = """
version: 2
models:
  - name: initial_view
    description: "This is a view"
    config:
      tblproperties:
        key: value
    columns:
      - name: id
        description: "This is the id column"
      - name: msg
"""

altered_view_sql = """
{{ config(materialized='view') }}
select id from {{ ref('seed') }};
"""


view_without_tags_sql = """
{{ config(materialized='view') }}

select cast(1 as bigint) as id
"""

view_with_tags_sql = """
{{ config(
    materialized='view',
    databricks_tags={'classification': 'internal'}
) }}

select cast(1 as bigint) as id
"""


qualified_star_view_sql = """
{{ config(materialized='view') }}
select src.* from {{ ref('seed') }} as src;
"""

seed_with_extra_csv = """id,msg,extra
1,hello,a
2,goodbye,b
2,yo,c
3,anyway,d
"""

contract_view_sql = """
{{ config(materialized='view') }}
select cast(1 as bigint) as id, 'hello' as msg
"""

contract_violating_view_sql = """
{{ config(materialized='view') }}
select 'one' as id, 'hello' as msg
"""

contract_schema_yml = """
version: 2
models:
  - name: contract_view
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: bigint
      - name: msg
        data_type: string
"""

contract_changed_type_schema_yml = contract_schema_yml.replace(
    "data_type: bigint", "data_type: string"
)

column_mask_schema_yml = """
version: 2
models:
  - name: contract_view
    columns:
      - name: id
      - name: msg
        column_mask:
          function: mask_msg
"""

row_filter_schema_yml = """
version: 2
models:
  - name: contract_view
    config:
      row_filter:
        function: filter_id
        columns: [id]
    columns:
      - name: id
      - name: msg
"""
