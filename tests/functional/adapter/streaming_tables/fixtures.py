from typing import Optional

from dbt.adapters.base.relation import BaseRelation

from dbt.adapters.databricks.relation import DatabricksRelationType


def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
    table_type = project.run_sql(
        "select table_type from `system`.`information_schema`.`tables`"
        f" where table_catalog = '{relation.database}'"
        f" and table_schema = '{relation.schema}'"
        f" and table_name = '{relation.identifier}'",
        fetch="one",
    )[0]
    if table_type == "STREAMING_TABLE":
        return DatabricksRelationType.StreamingTable
    elif table_type == "MANAGED" or table_type == "EXTERNAL":
        return DatabricksRelationType.Table
    else:
        is_materialized = project.run_sql(
            "select is_materialized from `system`.`information_schema`.`views`"
            f" where table_catalog = '{relation.database}'"
            f" and table_schema = '{relation.schema}'"
            f" and table_name = '{relation.identifier}'",
            fetch="one",
        )[0]
        if is_materialized == "TRUE":
            return DatabricksRelationType.MaterializedView
        else:
            return DatabricksRelationType.View


streaming_table = """
{{ config(
    materialized='streaming_table',
) }}
select * from stream {{ ref('my_seed') }}
"""

complex_streaming_table = """
{{ config(
    materialized='streaming_table',
    partition_by='id',
    schedule = {
        'cron': '0 0 * * * ? *',
        'time_zone': 'Etc/UTC'
    },
    tblproperties={
        'key': 'value'
    },
) }}
select * from stream {{ ref('my_seed') }}
"""

streaming_table_schema = """
version: 2

models:
  - name: my_streaming_table
    columns:
      - name: id
        description: "The unique identifier for each record"
        constraints:
          - type: not_null
      - name: value
    config:
      persist_docs:
        relation: true
        columns: true
"""


# There is a single CSV file in the source directory. The contents exactly matches MY_SEED
streaming_table_from_file = """
{{ config(
    materialized='streaming_table',
) }}
select * from stream read_files(
    '{{ env_var('DBT_DATABRICKS_LOCATION_ROOT') }}/test_inputs/streaming_table_test_sources'
);
"""

complex_types_streaming_table = """
{{ config(
    materialized='streaming_table',
) }}
select * from stream complex_types_table
"""
