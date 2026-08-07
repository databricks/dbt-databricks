liquid_cluster_sql = """
{{ config(materialized='incremental', liquid_clustered_by='id') }}
select 1 as id, 'Joe' as name
"""

auto_liquid_cluster_sql = """
{{ config(materialized='incremental', auto_liquid_cluster=true) }}
select 1 as id, 'Joe' as name
"""

table_liquid_cluster_sql = """
{{ config(materialized='table', liquid_clustered_by='id') }}
select 1 as id, 'Joe' as name
"""

auto_liquid_cluster_table_sql = """
{{ config(materialized='table', auto_liquid_cluster=true) }}
select 1 as id, 'Joe' as name
"""

table_skip_optimize_sql = """
{{ config(materialized='table', liquid_clustered_by='id', skip_optimize=true) }}
select 1 as id, 'Joe' as name
"""

incremental_skip_optimize_sql = """
{{ config(materialized='incremental', liquid_clustered_by='id', skip_optimize=true) }}
select 1 as id, 'Joe' as name
"""

incremental_three_cols_sql = """
{{ config(materialized='incremental') }}
select 1 as id, 'hello' as msg, 'blue' as color
"""

cluster_switch_schema_cols = """
version: 2

models:
  - name: cluster_switch
    config:
      liquid_clustered_by: id
"""

cluster_switch_schema_auto = """
version: 2

models:
  - name: cluster_switch
    config:
      auto_liquid_cluster: true
"""

cluster_switch_schema_plain = """
version: 2

models:
  - name: cluster_switch
"""

cluster_change_schema_initial = """
version: 2

models:
  - name: cluster_change
    config:
      liquid_clustered_by: id
"""

cluster_change_schema_updated = """
version: 2

models:
  - name: cluster_change
    config:
      liquid_clustered_by: ["msg", "color"]
"""
