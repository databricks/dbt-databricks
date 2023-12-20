from dataclasses import dataclass, field
from typing import Any, Dict, List, ClassVar
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.exceptions import DbtRuntimeError


@dataclass(frozen=True)
class TblPropertiesConfig(DatabricksComponentConfig):
    tblproperties: Dict[str, str] = field(default_factory=dict)
    ignore_list: List[str] = field(default_factory=list)
    to_unset: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if any([key in self.tblproperties for key in self.to_unset]):
            raise DbtRuntimeError("Cannot set and unset the same tblproperty in the same model")

    def _format_tblproperties(self, to_format: Dict[str, str]) -> str:
        return ", ".join(f"'{k}' = '{v}'" for k, v in to_format.items())

    def to_sql_clause(self) -> str:
        without_ignore_list = self._without_ignore_list(self.tblproperties)
        if without_ignore_list:
            return f"TBLPROPERTIES ({self._format_tblproperties(without_ignore_list)})"
        return ""

    def to_alter_sql_clauses(self) -> List[str]:
        """For now this is not called because MVs do not currently allow altering tblproperties.
        When that changes, switch to Alterable config to start invoking this method."""
        clauses = []
        without_ignore_list = self._without_ignore_list(self.tblproperties)
        if without_ignore_list:
            clauses.append(f"SET TBLPROPERTIES ({self._format_tblproperties(without_ignore_list)})")
        if self.to_unset:
            to_unset = ", ".join(f"'{k}'" for k in self.to_unset)
            clauses.append(f"UNSET TBLPROPERTIES IF EXISTS ({to_unset})")
        return clauses

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, TblPropertiesConfig):
            return False
        return (
            self._without_ignore_list(self.tblproperties)
            == self._without_ignore_list(other.tblproperties)
            and self.to_unset == other.to_unset
        )

    def _without_ignore_list(self, tblproperties: Dict[str, str]) -> Dict[str, str]:
        if tblproperties:
            ignore_list = self.ignore_list or ["pipelines.pipelineId"]
            return {k: v for k, v in tblproperties.items() if k not in ignore_list}
        return tblproperties

    def get_diff(self, other: DatabricksComponentConfig) -> "TblPropertiesConfig":
        if not isinstance(other, TblPropertiesConfig):
            raise DbtRuntimeError(
                f"Cannot diff {self.__class__.__name__} with {other.__class__.__name__}"
            )

        tblproperties = self._without_ignore_list(self.tblproperties)
        other_tblproperties = self._without_ignore_list(other.tblproperties)

        tblproperties = {
            k: v
            for k, v in tblproperties.items()
            if k not in other_tblproperties or v != other_tblproperties[k]
        }

        to_unset = []
        for k in other_tblproperties.keys():
            if k not in self.tblproperties:
                to_unset.append(k)

        return TblPropertiesConfig(
            tblproperties=tblproperties,
            ignore_list=self.ignore_list,
            to_unset=to_unset,
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

        return TblPropertiesConfig(tblproperties)

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> TblPropertiesConfig:
        tblproperties_config = model_node.config.extra.get("tblproperties_config")
        if tblproperties_config:
            ignore_list = tblproperties_config.get("ignore_list")
            tblproperties = tblproperties_config.get("tblproperties")
            if isinstance(tblproperties, Dict):
                tblproperties = {str(k): str(v) for k, v in tblproperties.items()}
            elif tblproperties is None:
                tblproperties = dict()
            else:
                raise DbtRuntimeError(
                    "If tblproperties_config is set, tblproperties must be a dictionary"
                )

            if ignore_list is None:
                return TblPropertiesConfig(tblproperties)

            if isinstance(ignore_list, List):
                ignore_list = [str(i) for i in ignore_list]
                return TblPropertiesConfig(tblproperties, ignore_list)
            else:
                raise DbtRuntimeError("ignore_list must be a list")

        return TblPropertiesConfig()
