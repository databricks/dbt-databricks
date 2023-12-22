from typing import Optional
from dbt.adapters.base import BaseRelation

from dbt.adapters.databricks.relation import DatabricksRelationType


def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
    table_type = project.run_sql(
        f"select table_type from {relation.information_schema_only()}."
        f"`tables` where table_name = '{relation.identifier}'",
        fetch="one",
    )[0]
    if table_type == "STREAMING_TABLE":
        return DatabricksRelationType.StreamingTable.value
    elif table_type == "MANAGED" or table_type == "EXTERNAL":
        return DatabricksRelationType.Table.value
    else:
        is_materialized = project.run_sql(
            f"select is_materialized from {relation.information_schema_only()}."
            f"`views` where table_name = '{relation.identifier}'",
            fetch="one",
        )[0]
        if is_materialized == "TRUE":
            return DatabricksRelationType.MaterializedView.value
        else:
            return DatabricksRelationType.View.value


materialized_view = """
{{ config(
    materialized='materialized_view',
    description='this is a materialized view',
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
