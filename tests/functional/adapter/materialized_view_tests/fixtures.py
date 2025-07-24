from typing import Optional

from dbt.adapters.base import BaseRelation
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


materialized_view = """
{{ config(
    materialized='materialized_view',
    partition_by='id',
    schedule = {
        'cron': '0 0 * * * ? *',
        'time_zone': 'Etc/UTC'
    },
    tblproperties={
        'key': 'value'
    },
) }}
select * from {{ ref('my_seed') }}
"""

materialized_view_schema = """
version: 2

models:
  - name: my_materialized_view
    columns:
      - name: id
        description: "The unique identifier for each record"
        constraints:
          - type: not_null
      - name: value
    constraints:
        - type: primary_key
          columns: [id]
    config:
      persist_docs:
        relation: true
        columns: true
"""

complex_types_materialized_view = """
{{ config(
    materialized='materialized_view',
    schedule = {
        'cron': '0 0 * * * ? *',
        'time_zone': 'Etc/UTC'
    },
) }}
SELECT
  named_struct(
    'field1', map('a', 1, 'b', 2),
    'field2', array(10, 20, 30),
    'field3', 3,
    'field4', 4,
    'field5', 5,
    'field6', 6,
    'field7', 7,
    'field8', 8,
    'field9', 9,
    'field10', 10,
    'field11', 11,
    'field12', 12,
    'field13', 13,
    'field14', 14,
    'field15', 15,
    'field16', 16,
    'field17', 17,
    'field18', 18,
    'field19', 19,
    'field20', 20,
    'field21', 21,
    'field22', 22,
    'field23', 23,
    'field24', 24,
    'field25', 25,
    'field26', 26,
    'field27', 27,
    'field28', 28,
    'field29', 29,
    'field30', 30
  ) AS my_struct;
"""
