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
        'time_zone_value': 'Etc/UTC'
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
        data_type: bigint
        description: "The unique identifier for each record"
        constraints:
          - type: not_null
      - name: value
        data_type: bigint
    config:
      contract:
        enforced: true
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

liquid_clustered_st = """
{{ config(
    materialized='streaming_table',
) }}
select * from stream {{ ref('my_seed') }}
"""

liquid_clustered_st_schema_v1 = """
version: 2

models:
  - name: liquid_clustered_st
    config:
      liquid_clustered_by: id
"""

liquid_clustered_st_schema_v2 = """
version: 2

models:
  - name: liquid_clustered_st
    config:
      liquid_clustered_by: [id, value]
"""

liquid_clustered_st_schema_v3 = """
version: 2

models:
  - name: liquid_clustered_st
    config:
      liquid_clustered_by: []
"""


def streaming_table_with_every(every_value: str) -> str:
    """Render a streaming-table model with `schedule = {'every': <value>}`."""
    return f"""
{{{{ config(
    materialized='streaming_table',
    schedule = {{'every': '{every_value}'}},
) }}}}
select * from stream {{{{ ref('my_seed') }}}}
"""


EVERY_ACCEPTED_INPUTS: list[str] = ["2 HOURS", "1 DAY", "4 WEEKS"]


streaming_table_on_update_bare = """
{{ config(
    materialized='streaming_table',
    schedule = {'on_update': True},
) }}
select * from stream {{ ref('my_seed') }}
"""

streaming_table_on_update_rate_limited = """
{{ config(
    materialized='streaming_table',
    schedule = {'on_update': True, 'at_most_every': '15 MINUTES'},
) }}
select * from stream {{ ref('my_seed') }}
"""

streaming_table_cron_no_tz = """
{{ config(
    materialized='streaming_table',
    schedule = {'cron': '0 0 * * * ? *'},
) }}
select * from stream {{ ref('my_seed') }}
"""

streaming_table_every_with_tblproperties = """
{{ config(
    materialized='streaming_table',
    schedule = {'every': '2 HOURS'},
    tblproperties={'lifecycle_marker': 'v1'},
) }}
select * from stream {{ ref('my_seed') }}
"""
