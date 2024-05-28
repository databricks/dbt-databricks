merge_update_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    merge_update_columns = ['msg'],
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- msg will be updated, color will be ignored
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

no_comment_schema = """
version: 2

models:
  - name: merge_update_columns_sql
    columns:
        - name: id
        - name: msg
        - name: color
"""

comment_schema = """
version: 2

models:
  - name: merge_update_columns_sql
    columns:
        - name: id
          description: This is the id column
        - name: msg
          description: This is the msg column
        - name: color
"""

tags_a = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        databricks_tags:
            a: b
            c: d
    columns:
        - name: id
        - name: msg
        - name: color
"""

tags_b = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        databricks_tags:
            c: e
            d: f
    columns:
        - name: id
        - name: msg
        - name: color
"""

liquid_clustering_a = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        liquid_clustered_by: id
"""

liquid_clustering_b = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        liquid_clustered_by: ["msg", "color"]
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = dbt.type_string() %}

{% if is_incremental() %}

SELECT id,
       cast(field4 as {{string_type}}) AS field4, -- to validate new field and order change
       cast(field2 as {{string_type}}) as field2,
       cast(field1 as {{string_type}}) as field1  -- to validate order change

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2

from source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as {{string_type}}) as field1
       ,field2
       ,cast(case when id <= 3 then null else field4 end as {{string_type}}) as field4

from source_data
order by id
"""

models__databricks_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- merge will not happen on the above record where id = 2, so new record will fall to insert
select cast(1 as bigint) as id, 'hey' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

append_expected = """id,msg
1,hello
2,goodbye
2,yo
3,anyway
"""

overwrite_expected = """id,msg
2,yo
3,anyway
"""

upsert_expected = """id,msg
1,hello
2,yo
3,anyway
"""

partial_upsert_expected = """id,msg,color
1,hello,blue
2,yo,red
3,anyway,purple
"""

exclude_upsert_expected = """id,msg,color
1,hello,blue
2,goodbye,green
3,anyway,purple
"""

replace_where_expected = """id,msg,color
1,hello,blue
3,anyway,purple
"""

base_model = """
{{ config(
    materialized = 'incremental'
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

upsert_model = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- msg will be ignored, color will be updated
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}"""

replace_where_model = """
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
"""


simple_python_model = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='incremental',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""

python_schema = """version: 2
models:
  - name: tags
    config:
      tags: ["python"]
      databricks_tags:
        a: b
        c: d
      http_path: "{{ env_var('DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH') }}"
"""

python_schema2 = """version: 2
models:
  - name: tags
    config:
      tags: ["python"]
      databricks_tags:
        c: e
        d: f
      http_path: "{{ env_var('DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH') }}"
"""

lc_python_schema = """version: 2
models:
  - name: simple_python_model
    config:
      liquid_clustered_by: test
      http_path: "{{ env_var('DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH') }}"
"""

lc_python_schema2 = """version: 2
models:
  - name: simple_python_model
    config:
      liquid_clustered_by: test2
      http_path: "{{ env_var('DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH') }}"
"""
