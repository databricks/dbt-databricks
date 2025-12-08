liquid_cluster_sql = """
{{ config(materialized='incremental', liquid_clustered_by='id') }}
select 1 as id, 'Joe' as name
"""

auto_liquid_cluster_sql = """
{{ config(materialized='incremental', auto_liquid_cluster=true) }}
select 1 as id, 'Joe' as name
"""
