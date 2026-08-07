catalog_defaulted_iceberg = """
{{ config(materialized='table', catalog_name='iceberg_uniform_catalog') }}
select 1 as id, 'uniform' as name
"""
