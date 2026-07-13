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
        'time_zone_value': 'Etc/UTC'
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
        data_type: bigint
        description: "The unique identifier for each record"
        constraints:
          - type: not_null
      - name: value
        data_type: bigint
    constraints:
        - type: primary_key
          columns: [id]
    config:
      contract:
        enforced: true
      persist_docs:
        relation: true
        columns: true
"""

complex_types_materialized_view = """
{{ config(
    materialized='materialized_view',
    schedule = {
        'cron': '0 0 * * * ? *',
        'time_zone_value': 'Etc/UTC'
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

liquid_clustered_mv = """
{{ config(
    materialized='materialized_view',
) }}
select * from {{ ref('my_seed') }}
"""

liquid_clustered_mv_schema_v1 = """
version: 2

models:
  - name: liquid_clustered_mv
    config:
      liquid_clustered_by: id
"""

liquid_clustered_mv_schema_v2 = """
version: 2

models:
  - name: liquid_clustered_mv
    config:
      liquid_clustered_by: [id, value]
"""

liquid_clustered_mv_schema_v3 = """
version: 2

models:
  - name: liquid_clustered_mv
    config:
      liquid_clustered_by: []
"""


def materialized_view_with_every(every_value: str) -> str:
    """Render an MV model with `schedule = {'every': <value>}`."""
    return f"""
{{{{ config(
    materialized='materialized_view',
    schedule = {{'every': '{every_value}'}},
) }}}}
select * from {{{{ ref('my_seed') }}}}
"""


EVERY_ACCEPTED_INPUTS: list[str] = ["2 HOURS", "1 DAY", "4 WEEKS"]


materialized_view_on_update_bare = """
{{ config(
    materialized='materialized_view',
    schedule = {'on_update': True},
) }}
select * from {{ ref('my_seed') }}
"""

materialized_view_on_update_rate_limited = """
{{ config(
    materialized='materialized_view',
    schedule = {'on_update': True, 'at_most_every': '15 MINUTES'},
) }}
select * from {{ ref('my_seed') }}
"""

materialized_view_cron_no_tz = """
{{ config(
    materialized='materialized_view',
    schedule = {'cron': '0 0 * * * ? *'},
) }}
select * from {{ ref('my_seed') }}
"""

materialized_view_no_schedule = """
{{ config(materialized='materialized_view') }}
select * from {{ ref('my_seed') }}
"""

materialized_view_every_with_tblproperties = """
{{ config(
    materialized='materialized_view',
    schedule = {'every': '2 HOURS'},
    tblproperties={'lifecycle_marker': 'v1'},
) }}
select * from {{ ref('my_seed') }}
"""


metadata_fetch_mv_seed_csv = """id,value
1,100
2,200
""".lstrip()

metadata_fetch_materialized_view_without_tags_sql = """
{{ config(
    materialized='materialized_view',
) }}
select * from {{ ref('mv_metadata_fetch_seed') }}
"""

metadata_fetch_materialized_view_with_tags_sql = """
{{ config(
    materialized='materialized_view',
    databricks_tags={'classification': 'internal'},
) }}
select * from {{ ref('mv_metadata_fetch_seed') }}
"""

materialized_view_streaming_source_seed_csv = """id,value
1,100
""".lstrip()

materialized_view_streaming_source_table_sql = """
{{ config(materialized='streaming_table') }}
select * from stream {{ ref('materialized_view_streaming_source_seed') }}
"""

materialized_view_streaming_source_sql = """
{{ config(materialized='materialized_view') }}
select * from {{ ref('materialized_view_streaming_source_table') }}
"""


mv_norebuild_seed_csv = """id,value
1,100
2,200
""".lstrip()

# Updateable-change / no-rebuild fixtures: an identical query whose only difference
# step-to-step is exactly one updateable component (tags, then refresh schedule).
# The MV starts MANUAL (no schedule) and the refresh step moves it to EVERY 4 WEEKS.
mv_norebuild_v1 = """
{{ config(
    materialized='materialized_view',
    on_configuration_change='apply',
    databricks_tags={'lifecycle': 'a'},
) }}
select * from {{ ref('mv_norebuild_seed') }}
"""

mv_norebuild_v2_tag_changed = """
{{ config(
    materialized='materialized_view',
    on_configuration_change='apply',
    databricks_tags={'lifecycle': 'a', 'extra': 'b'},
) }}
select * from {{ ref('mv_norebuild_seed') }}
"""

mv_norebuild_v3_refresh_changed = """
{{ config(
    materialized='materialized_view',
    on_configuration_change='apply',
    databricks_tags={'lifecycle': 'a', 'extra': 'b'},
    schedule={'every': '4 WEEKS'},
) }}
select * from {{ ref('mv_norebuild_seed') }}
"""
