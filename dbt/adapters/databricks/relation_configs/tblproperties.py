from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class TblPropertiesConfig(DatabricksComponentConfig):
    """Component encapsulating the tblproperties of a relation."""

    tblproperties: dict[str, str]
    pipeline_id: Optional[str] = None

    def get_diff(self, other: "TblPropertiesConfig") -> Optional["TblPropertiesConfig"]:
        # tblproperties are "set only" - we never unset tblproperties, only add or update them
        if unapplied_properties := self.tblproperties.items() - other.tblproperties.items():
            return TblPropertiesConfig(tblproperties={k: v for k, v in unapplied_properties})
        return None


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
                else:
                    tblproperties[str(row[0])] = str(row[1])

        return TblPropertiesConfig(tblproperties=tblproperties, pipeline_id=pipeline_id)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> TblPropertiesConfig:
        tblproperties = base.get_config_value(relation_config, "tblproperties") or {}

        if not isinstance(tblproperties, dict):
            raise DbtRuntimeError("tblproperties must be a dictionary")

        table_format = base.get_config_value(relation_config, "table_format")
        use_managed_iceberg = GlobalState.get_use_managed_iceberg()

        is_uniform = table_format == constants.ICEBERG_TABLE_FORMAT and use_managed_iceberg is False

        if is_uniform:
            tblproperties.update(
                {
                    "delta.enableIcebergCompatV2": "true",
                    "delta.universalFormat.enabledFormats": constants.ICEBERG_TABLE_FORMAT,
                }
            )

        tblproperties = {str(k): str(v) for k, v in tblproperties.items()}
        return TblPropertiesConfig(tblproperties=tblproperties)
