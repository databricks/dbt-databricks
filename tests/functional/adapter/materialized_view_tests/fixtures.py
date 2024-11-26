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
