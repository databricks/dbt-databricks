import dataclasses
from typing import Any, Optional

from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfigBase
from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class DatabricksAdapterIsUniformParams:
    thread_id: str
    config: Any

    def _to_dict(self):
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

    def _to_dict(self):
        from dbt.adapters.record.serialization import serialize_base_relation

        return {
            "thread_id": self.thread_id,
            "relation": serialize_base_relation(self.relation),
            "model_config": self.model_config.model_dump() if self.model_config else None,
        }


@dataclasses.dataclass
class DatabricksAdapterGetRelationConfigResult:
    return_val: Optional[dict]  # DatabricksRelationConfigBase serialized as dict, or None

    def _to_dict(self):
        # return_val is already converted to dict by the constructor
        return {
            "return_val": self.return_val,
        }

    def __init__(self, return_val):
        if return_val is not None and not isinstance(return_val, dict):
            self.return_val = return_val.model_dump()
        else:
            self.return_val = return_val

    @classmethod
    def _from_dict(cls, data):
        return cls(return_val=data.get("return_val"))


@Recorder.register_record_type
class DatabricksAdapterGetRelationConfigRecord(Record):
    """Implements record/replay support for the DatabricksAdapter.get_relation_config() method."""

    params_cls = DatabricksAdapterGetRelationConfigParams
    result_cls = DatabricksAdapterGetRelationConfigResult
    group = "Available"
