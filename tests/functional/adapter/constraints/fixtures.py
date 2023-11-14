from dbt.tests.adapter.constraints import fixtures

# constraints are enforced via 'alter' statements that run after table creation
expected_sql = """
create or replace table <model_identifier>
    using delta
    as
select
  id,
  color,
  date_day
from
( select
    'blue' as color,
    1 as id,
    '2019-01-01' as date_day ) as model_subq
"""

# Different on Spark:
# - does not support a data type named 'text'
# (TODO handle this in the base test classes using string_type
constraints_yml = fixtures.model_schema_yml.replace("text", "string").replace("primary key", "")
