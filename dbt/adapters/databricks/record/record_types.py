import dataclasses
from typing import Any, Optional

from dbt.adapters.record.serialization import serialize_bindings
from dbt_common.record import Record, Recorder

from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfigBase


@dataclasses.dataclass
class DatabricksAdapterAddQueryParams:
    thread_id: str
    sql: str
    auto_begin: bool = True
    bindings: Optional[Any] = None
    abridge_sql_log: bool = False
    close_cursor: bool = False

    def _to_dict(self) -> dict[str, Any]:
        return {
            "thread_id": self.thread_id,
            "sql": self.sql,
            "auto_begin": self.auto_begin,
            "bindings": serialize_bindings(self.bindings),
            "abridge_sql_log": self.abridge_sql_log,
            "close_cursor": self.close_cursor,
        }


@dataclasses.dataclass
class DatabricksAdapterAddQueryResult:
    return_val: tuple

    def _to_dict(self) -> dict[str, Any]:
        return {
            "return_val": {
                "conn": "conn",
                "cursor": "cursor",
            }
        }


@Recorder.register_record_type
class DatabricksAdapterAddQueryRecord(Record):
    """Implements record/replay support for the DatabricksAdapter.add_query() method."""

    params_cls = DatabricksAdapterAddQueryParams
    result_cls = DatabricksAdapterAddQueryResult
    group = "Available"


@dataclasses.dataclass
class DatabricksAdapterIsUniformParams:
    thread_id: str
    config: Any

    def _to_dict(self) -> dict[str, Any]:
        return {
            "thread_id": self.thread_id,
            "config_model": self.config.model.to_dict(),
            "materialized": self.config.get("materialized"),
        }


@dataclasses.dataclass
class DatabricksAdapterIsUniformResult:
    return_val: bool


@Recorder.register_record_type
class DatabricksAdapterIsUniformRecord(Record):
    """Implements record/replay support for the DatabricksAdapter.is_uniform() method."""

    params_cls = DatabricksAdapterIsUniformParams
    result_cls = DatabricksAdapterIsUniformResult
    group = "Available"


@dataclasses.dataclass
class DatabricksAdapterGetRelationConfigParams:
    thread_id: str
    relation: DatabricksRelation
    model_config: Optional[DatabricksRelationConfigBase]

    def _to_dict(self) -> dict[str, Any]:
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
            "model_config": (
                {
                    "config": {
                        k: v.model_dump(mode="json")
                        for k, v in self.model_config.config.items()
                    }
                }
                if self.model_config
                else None
            ),
        }


@dataclasses.dataclass
class DatabricksAdapterGetRelationConfigResult:
    return_val: Optional[dict]  # DatabricksRelationConfigBase serialized as dict, or None

    def _to_dict(self) -> dict[str, Any]:
        # return_val is already converted to dict by the constructor
        return {
            "return_val": self.return_val,
        }

    def __init__(self, return_val: Optional[Any]) -> None:
        if return_val is not None and not isinstance(return_val, dict):
            self.return_val = {
                "config": {k: v.model_dump(mode="json") for k, v in return_val.config.items()}
            }
        else:
            self.return_val = return_val

    @classmethod
    def _from_dict(cls, data: dict[str, Any]) -> "DatabricksAdapterGetRelationConfigResult":
        return cls(return_val=data.get("return_val"))


@Recorder.register_record_type
class DatabricksAdapterGetRelationConfigRecord(Record):
    """Implements record/replay support for the DatabricksAdapter.get_relation_config() method."""

    params_cls = DatabricksAdapterGetRelationConfigParams
    result_cls = DatabricksAdapterGetRelationConfigResult
    group = "Available"
