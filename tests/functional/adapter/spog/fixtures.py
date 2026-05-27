spog_smoke_model = """
select 1 as id
"""


spog_named_compute_model = """
{{ config(databricks_compute='alternate_uc_cluster') }}

select 1 as id
"""
