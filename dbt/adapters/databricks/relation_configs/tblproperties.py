from typing import Any
from typing import ClassVar
from typing import Dict
from typing import List

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentConfig
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentProcessor
from dbt.adapters.relation_configs.config_base import RelationResults


class TblPropertiesConfig(DatabricksComponentConfig):
    """Component encapsulating the tblproperties of a relation."""

    tblproperties: Dict[str, str]

    # List of tblproperties that should be ignored when comparing configs. These are generally
    # set by Databricks and are not user-configurable.
    ignore_list: List[str] = [
        "pipelines.pipelineId",
        "delta.enableChangeDataFeed",
        "delta.minReaderVersion",
        "delta.minWriterVersion",
        "pipeline_internal.catalogType",
        "pipelines.metastore.tableName",
        "pipeline_internal.enzymeMode",
    ]

    def __eq__(self, __value: Any) -> bool:
        """Override equality check to ignore certain tblproperties."""

        if not isinstance(__value, TblPropertiesConfig):
            return False

        def _without_ignore_list(d: Dict[str, str]) -> Dict[str, str]:
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

        if table:
            for row in table.rows:
                tblproperties[str(row[0])] = str(row[1])

        return TblPropertiesConfig(tblproperties=tblproperties)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> TblPropertiesConfig:
        tblproperties = base.get_config_value(relation_config, "tblproperties")
        if not tblproperties:
            return TblPropertiesConfig(tblproperties=dict())
        if isinstance(tblproperties, Dict):
            tblproperties = {str(k): str(v) for k, v in tblproperties.items()}
            return TblPropertiesConfig(tblproperties=tblproperties)
        else:
            raise DbtRuntimeError("tblproperties must be a dictionary")
