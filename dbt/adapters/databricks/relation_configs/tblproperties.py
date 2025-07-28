from typing import Any, ClassVar, Optional

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults


class TblPropertiesConfig(DatabricksComponentConfig):
    """Component encapsulating the tblproperties of a relation."""

    tblproperties: dict[str, str]
    pipeline_id: Optional[str] = None

    # List of tblproperties that should be ignored when comparing configs. These are generally
    # set by Databricks and are not user-configurable.
    ignore_list: ClassVar[list[str]] = [
        "pipelines.pipelineId",
        "delta.enableChangeDataFeed",
        "delta.minReaderVersion",
        "delta.minWriterVersion",
        "pipeline_internal.catalogType",
        "pipelines.metastore.tableName",
        "pipeline_internal.enzymeMode",
        "clusterByAuto",
        "clusteringColumns",
        "delta.enableRowTracking",
        "delta.feature.appendOnly",
        "delta.feature.changeDataFeed",
        "delta.feature.checkConstraints",
        "delta.feature.domainMetadata",
        "delta.feature.generatedColumns",
        "delta.feature.invariants",
        "delta.feature.rowTracking",
        "delta.rowTracking.materializedRowCommitVersionColumnName",
        "delta.rowTracking.materializedRowIdColumnName",
        "spark.internal.pipelines.top_level_entry.user_specified_name",
        "delta.columnMapping.maxColumnId",
        "spark.sql.internal.pipelines.parentTableId",
        "delta.enableDeletionVectors",
        "delta.feature.deletionVectors",
    ]

    def __eq__(self, __value: Any) -> bool:
        """Override equality check to ignore certain tblproperties."""

        if not isinstance(__value, TblPropertiesConfig):
            return False

        def _without_ignore_list(d: dict[str, str]) -> dict[str, str]:
            return {k: v for k, v in d.items() if k not in self.ignore_list}

        return _without_ignore_list(self.tblproperties) == _without_ignore_list(
            __value.tblproperties
        )


class TblPropertiesProcessor(DatabricksComponentProcessor[TblPropertiesConfig]):
    name: ClassVar[str] = "tblproperties"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> TblPropertiesConfig:
        table = results.get("show_tblproperties")
        tblproperties = dict()
        pipeline_id = None

        if table:
            for row in table.rows:
                if str(row[0]) == "pipelines.pipelineId":
                    pipeline_id = str(row[1])
                elif str(row[0]) not in TblPropertiesConfig.ignore_list:
                    tblproperties[str(row[0])] = str(row[1])

        return TblPropertiesConfig(tblproperties=tblproperties, pipeline_id=pipeline_id)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> TblPropertiesConfig:
        tblproperties = base.get_config_value(relation_config, "tblproperties") or {}
        is_iceberg = base.get_config_value(relation_config, "table_format") == "iceberg"

        if not isinstance(tblproperties, dict):
            raise DbtRuntimeError("tblproperties must be a dictionary")

        # If the table format is Iceberg, we need to set the iceberg-specific tblproperties
        if is_iceberg:
            tblproperties.update(
                {
                    "delta.enableIcebergCompatV2": "true",
                    "delta.universalFormat.enabledFormats": "iceberg",
                }
            )
        tblproperties = {str(k): str(v) for k, v in tblproperties.items()}
        return TblPropertiesConfig(tblproperties=tblproperties)
