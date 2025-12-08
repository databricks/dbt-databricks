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

invalid_iceberg_view = """
{{
  config(
    materialized = "view",
    table_format = "iceberg",
  )
}}
select 1 as id
"""

invalid_iceberg_format = """
{{
  config(
    materialized = "table",
    table_format = "iceberg",
    file_format = "parquet",
  )
}}
select 1 as id
"""

incremental_iceberg_base = """
{{
  config(
    materialized = "incremental",
    table_format = "iceberg",
    incremental_strategy = "merge",
    unique_key = "id",
  )
}}
select 1 as id, 'initial' as status
"""

incremental_iceberg_update = """
{{
  config(
    materialized = "incremental",
    table_format = "iceberg",
    incremental_strategy = "merge",
    unique_key = "id",
  )
}}
select 1 as id, 'updated' as status
union all
select 2 as id, 'new' as status
"""
