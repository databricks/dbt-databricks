"""
Core data models for the Databricks adapter.

This module contains the primary data structures used throughout the adapter,
including configuration models and relation information models.
"""

from dataclasses import dataclass
from typing import NamedTuple, Optional, Union

from dbt.adapters.base import AdapterConfig
from dbt.adapters.databricks.relation_configs.table_format import TableFormat


class DatabricksRelationInfo(NamedTuple):
    """Information about a Databricks relation (table/view)."""

    table_name: str
    table_type: str
    file_format: Optional[str]
    table_owner: Optional[str]
    databricks_table_type: Optional[str]


@dataclass
class DatabricksConfig(AdapterConfig):
    """Configuration model for Databricks-specific adapter settings."""

    file_format: str = "delta"
    table_format: str = TableFormat.DEFAULT
    location_root: Optional[str] = None
    include_full_name_in_path: bool = False
    partition_by: Optional[Union[list[str], str]] = None
    clustered_by: Optional[Union[list[str], str]] = None
    liquid_clustered_by: Optional[Union[list[str], str]] = None
    auto_liquid_cluster: Optional[bool] = None
    buckets: Optional[int] = None
    options: Optional[dict[str, str]] = None
    merge_update_columns: Optional[str] = None
    merge_exclude_columns: Optional[str] = None
    databricks_tags: Optional[dict[str, str]] = None
    tblproperties: Optional[dict[str, str]] = None
    zorder: Optional[Union[list[str], str]] = None
    unique_tmp_table_suffix: bool = False
    skip_non_matched_step: Optional[bool] = None
    skip_matched_step: Optional[bool] = None
    matched_condition: Optional[str] = None
    not_matched_condition: Optional[str] = None
    not_matched_by_source_action: Optional[str] = None
    not_matched_by_source_condition: Optional[str] = None
    target_alias: Optional[str] = None
    source_alias: Optional[str] = None
    merge_with_schema_evolution: Optional[bool] = None
    use_safer_relation_operations: Optional[bool] = None
    incremental_apply_config_changes: Optional[bool] = None
    view_update_via_alter: Optional[bool] = None
