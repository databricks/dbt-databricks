source_table = """
{{ config(materialized='table') }}

select 1 as id, 100 as revenue, 'completed' as status, '2024-01-01' as order_date
union all
select 2 as id, 200 as revenue, 'pending' as status, '2024-01-02' as order_date
union all
select 3 as id, 150 as revenue, 'completed' as status, '2024-01-03' as order_date
"""

basic_metric_view = """
{{ config(materialized='metric_view') }}

version: 0.1
source: "{{ ref('source_orders') }}"
dimensions:
  - name: order_date
    expr: order_date
  - name: status
    expr: status
measures:
  - name: total_orders
    expr: count(1)
  - name: total_revenue
    expr: sum(revenue)
"""

metric_view_with_filter = """
{{ config(materialized='metric_view') }}

version: 0.1
source: "{{ ref('source_orders') }}"
filter: status = 'completed'
dimensions:
  - name: order_date
    expr: order_date
measures:
  - name: completed_orders
    expr: count(1)
  - name: completed_revenue
    expr: sum(revenue)
"""

metric_view_with_config = """
{{
  config(
    materialized='metric_view',
    databricks_tags={
      'team': 'analytics',
      'environment': 'test'
    }
  )
}}

version: 0.1
source: "{{ ref('source_orders') }}"
dimensions:
  - name: status
    expr: status
measures:
  - name: order_count
    expr: count(1)
"""