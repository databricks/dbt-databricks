from dataclasses import dataclass
from typing import Optional, Set

import agate
from dbt.adapters.relation_configs import (
    RelationResults,
    RelationConfigChange,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfigBase

from dbt.adapters.databricks.utils import evaluate_bool


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabricksMaterializedViewConfig(DatabricksRelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view.html

    The following parameters are configurable by dbt:
    - mv_name: name of the materialized view
    - query: the query that defines the view
    - partition:
    - schedule

    There are currently no non-configurable parameters.
    """

    mv_name: str
    schema_name: str
    database_name: str
    query: str
    backup: bool = True
    partition: None # to be done
    schedule: bool = False

    @property
    def path(self) -> str:
        return ".".join(
            part
            for part in [self.database_name, self.schema_name, self.mv_name]
            if part is not None
        )

    # can be filled out later
    # @property
    # def validation_rules(self) -> Set[RelationConfigValidationRule]:
    #     # sort and dist rules get run by default with the mixin
    #     return {}

    @classmethod
    def from_dict(cls, config_dict) -> "DatabricksMaterializedViewConfig":
        kwargs_dict = {
            "mv_name": cls._render_part(ComponentName.Identifier, config_dict.get("mv_name")),
            "schema_name": cls._render_part(ComponentName.Schema, config_dict.get("schema_name")),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict.get("database_name")
            ),
            "query": config_dict.get("query"),
            "schedule": config_dict.get("schedule"),
        }

        materialized_view: "DatabricksMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "mv_name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
        }

        autorefresh_value = model_node.config.extra.get("schedule")
        if autorefresh_value is not None:
            config_dict["schedule"] = evaluate_bool(autorefresh_value)

        if query := model_node.compiled_code:
            config_dict.update({"query": query.strip()})

        if model_node.config.get("partition"):
            config_dict.update({"partition": DatabricksPartitionConfig.parse_model_node(model_node)})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            relation_results: the description of the materialized view from the database in this format:

                {
                    "materialized_view": agate.Table(
                        agate.Row({
                            "database": "<database_name>",
                            "schema": "<schema_name>",
                            "table": "<name>",
                            "diststyle": "<diststyle/distkey>",  # e.g. EVEN | KEY(column1) | AUTO(ALL) | AUTO(KEY(id)),
                            "sortkey1": "<column_name>",
                            "autorefresh: any("t", "f"),
                        })
                    ),
                    "query": agate.Table(
                        agate.Row({"definition": "<query>")}
                    ),
                }

                Additional columns in either value is fine, as long as `sortkey` and `sortstyle` are available.

        Returns: a standard dictionary describing this `DatabricksMaterializedViewConfig` instance
        """
        materialized_view: agate.Row = cls._get_first_row(
            relation_results.get("materialized_view")
        )
        query: agate.Row = cls._get_first_row(relation_results.get("query"))

        config_dict = {
            "mv_name": materialized_view.get("table"),
            "schema_name": materialized_view.get("schema"),
            "database_name": materialized_view.get("database"),
            "autorefresh": materialized_view.get("autorefresh"),
            "query": cls._parse_query(query.get("definition")),
        }

        # the default for materialized views differs from the default for diststyle in general
        # only set it if we got a value
        if materialized_view.get("diststyle"):
            config_dict.update(
                {"dist": DatabricksDistConfig.parse_relation_results(materialized_view)}
            )

        # TODO: this only shows the first column in the sort key
        if materialized_view.get("sortkey1"):
            config_dict.update(
                {"sort": DatabricksSortConfig.parse_relation_results(materialized_view)}
            )

        return config_dict

# @dataclass(frozen=True, eq=True, unsafe_hash=True)
# class DatabricksAutoRefreshConfigChange(RelationConfigChange):
#     context: Optional[bool] = None

#     @property
#     def requires_full_refresh(self) -> bool:
#         return False



@dataclass
class DatabricksMaterializedViewConfigChangeset:
    dist: Optional[DatabricksPartitionConfigChange] = None
    autorefresh: Optional[DatabricksAutoRefreshConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.autorefresh.requires_full_refresh if self.autorefresh else False,
                self.backup.requires_full_refresh if self.backup else False,
                self.dist.requires_full_refresh if self.dist else False,
                self.sort.requires_full_refresh if self.sort else False,
            }
        )

    @property
    def has_changes(self) -> bool:
        return any(
            {
                self.backup if self.backup else False,
                self.dist if self.dist else False,
                self.sort if self.sort else False,
                self.autorefresh if self.autorefresh else False,
            }
        )
