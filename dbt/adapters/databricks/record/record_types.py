import dataclasses
from typing import Any

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
