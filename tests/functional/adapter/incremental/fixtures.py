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
    description: This is a model description
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

tblproperties_a = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        tblproperties:
            a: b
            c: d
    columns:
        - name: id
        - name: msg
        - name: color
"""

tblproperties_b = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        tblproperties:
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

liquid_clustering_c = """
version: 2

models:
  - name: merge_update_columns_sql
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

skip_matched_expected = """id,msg,color
1,hello,blue
2,goodbye,red
3,anyway,purple
"""

skip_not_matched_expected = """id,msg,color
1,hey,cyan
2,yo,green
"""

matching_condition_expected = """id,first,second,V
1,Jessica,Atreides,2
2,Paul,Atreides,1
3,Dunkan,Aidaho,1
4,Baron,Harkonnen,1
"""

not_matched_by_source_then_del_expected = """id,first,second,V
2,Paul,Atreides,0
3,Dunkan,Aidaho,1
4,Baron,Harkonnen,1
"""

not_matched_by_source_then_upd_expected = """id,first,second,V
1,--,--,-1
2,Paul,Atreides,0
3,Dunkan,Aidaho,1
4,Baron,Harkonnen,1
"""

merge_schema_evolution_expected = """id,first,second,V
1,Jessica,Atreides,1
2,Paul,Atreides,
3,Dunkan,Aidaho,2
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

skip_matched_model = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    skip_matched_step = true,
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

-- data for subsequent incremental update

select 1 as id, 'hey' as msg, 'cyan' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

skip_not_matched_model = skip_matched_model.replace(
    "skip_matched_step = true", "skip_not_matched_step = true"
)

matching_condition_model = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    source_alias='src',
    target_alias='t',
    matched_condition='src.V > t.V and hash(src.first, src.second) <> hash(t.first, t.second)',
    not_matched_condition='src.V > 0',
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'Vasya' as first, 'Pupkin' as second, 1 as V
union all
select 2 as id, 'Paul' as first, 'Atreides' as second, 1 as V
union all
select 3 as id, 'Dunkan' as first, 'Aidaho' as second, 1 as V

{% else %}

-- data for subsequent incremental update

select 1 as id, 'Jessica' as first, 'Atreides' as second, 2 as V -- should merge
union all
select 2 as id, 'Paul' as first, 'Whiskas' as second, 1 as V -- V is same, no merge
union all
select 3 as id, 'Dunkan' as first, 'Aidaho' as second, 2 as V -- Hash is same, no merge
union all
select 4 as id, 'Baron' as first, 'Harkonnen' as second, 1 as V -- should append
union all
select 5 as id, 'Raban' as first, '' as second, 0 as V -- no append

{% endif %}
"""

not_matched_by_source_then_delete_model = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    target_alias='t',
    source_alias='s',
    skip_matched_step=true,
    not_matched_by_source_condition='t.V > 0',
    not_matched_by_source_action='delete',
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'Vasya' as first, 'Pupkin' as second, 1 as V
union all
select 2 as id, 'Paul' as first, 'Atreides' as second, 0 as V
union all
select 3 as id, 'Dunkan' as first, 'Aidaho' as second, 1 as V

{% else %}

-- data for subsequent incremental update

-- id = 1 should be deleted
-- id = 2 should be kept as condition doesn't hold (t.V = 0)
select 3 as id, 'Dunkan' as first, 'Aidaho' as second, 2 as V -- No update, skipped
union all
select 4 as id, 'Baron' as first, 'Harkonnen' as second, 1 as V -- should append

{% endif %}
"""

not_matched_by_source_then_update_model = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    target_alias='t',
    source_alias='s',
    skip_matched_step=true,
    not_matched_by_source_condition='t.V > 0',
    not_matched_by_source_action='''
        update set
            t.first = \\\'--\\\',
            t.second = \\\'--\\\',
            t.V = -1
    ''',
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'Vasya' as first, 'Pupkin' as second, 1 as V
union all
select 2 as id, 'Paul' as first, 'Atreides' as second, 0 as V
union all
select 3 as id, 'Dunkan' as first, 'Aidaho' as second, 1 as V

{% else %}

-- data for subsequent incremental update

-- id = 1 should be updated with
-- id = 2 should be kept as condition doesn't hold (t.V = 0)
select 3 as id, 'Dunkan' as first, 'Aidaho' as second, 2 as V -- No update, skipped
union all
select 4 as id, 'Baron' as first, 'Harkonnen' as second, 1 as V -- should append

{% endif %}
"""

merge_schema_evolution_model = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_with_schema_evolution=true,
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'Vasya' as first, 'Pupkin' as second
union all
select 2 as id, 'Paul' as first, 'Atreides' as second
union all
select 3 as id, 'Dunkan' as first, 'Aidaho' as second

{% else %}

-- data for subsequent incremental update

select 1 as id, 'Jessica' as first, 'Atreides' as second, 1 as V
-- id = 2 should have NULL in V.
union all
select 3 as id, 'Dunkan' as first, 'Aidaho' as second, 2 as V

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

python_tblproperties_schema = """version: 2
models:
  - name: tblproperties
    config:
      tags: ["python"]
      tblproperties:
        a: b
        c: d
      http_path: "{{ env_var('DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH') }}"
"""

python_tblproperties_schema2 = """version: 2
models:
  - name: tblproperties
    config:
      tags: ["python"]
      tblproperties:
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

replace_table = """
{{ config(
    materialized = 'table'
) }}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color
"""

replace_incremental = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = 'id'
) }}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color
"""

replace_expected = """id,msg,color
1,hello,blue
2,goodbye,red
"""
