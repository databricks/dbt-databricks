basic_table = """
{{
  config(
    materialized = "table",
  )
}}
select 1 as id
"""

basic_iceberg = """
{{
  config(
    materialized = "table",
    table_format="iceberg",
  )
}}
select * from {{ ref('first_table') }}
"""

ref_iceberg = """
{{
  config(
    materialized = "table",
  )
}}
select * from {{ ref('iceberg_table') }}
"""

basic_view = """
select 1 as id
"""

basic_iceberg_swap = """
{{
  config(
    materialized = "table",
    table_format="iceberg",
  )
}}
select 1 as id
"""

basic_incremental_swap = """
{{
  config(
    materialized = "incremental",
  )
}}
select 1 as id
"""
