from typing import Any, Dict, ClassVar, List
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.exceptions import DbtRuntimeError


class TblPropertiesConfig(DatabricksComponentConfig):
    """Component encapsulating the tblproperties of a relation."""

    tblproperties: Dict[str, str]

    # List of tblproperties that should be ignored when comparing configs. These are generally
    # set by Databricks and are not user-configurable.
    ignore_list: List[str] = ["pipelines.pipelineId"]

    @property
    def requires_full_refresh(self) -> bool:
        # TODO: This is only True for MVs since they don't currently allow ALTER VIEW to change the
        # tblproperties. Should be False for tables and views, if and when they move to this
        # approach.
        return True

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
    def from_results(cls, results: RelationResults) -> TblPropertiesConfig:
        table = results.get("show_tblproperties")
        tblproperties = dict()

        if table:
            for row in table.rows:
                tblproperties[str(row[0])] = str(row[1])

        return TblPropertiesConfig(tblproperties=tblproperties)

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> TblPropertiesConfig:
        tblproperties = model_node.config.extra.get("tblproperties")
        if not tblproperties:
            return TblPropertiesConfig(tblproperties=dict())
        if isinstance(tblproperties, Dict):
            tblproperties = {str(k): str(v) for k, v in tblproperties.items()}
            return TblPropertiesConfig(tblproperties=tblproperties)
        else:
            raise DbtRuntimeError("tblproperties must be a dictionary")
