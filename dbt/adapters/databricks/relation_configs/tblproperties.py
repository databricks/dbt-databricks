from typing import Any, ClassVar, Optional

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


def _is_mock(obj: Any) -> bool:
    return hasattr(obj, "_mock_return_value") or hasattr(obj, "_mock_wraps")


def _get_config_attr(config: Any, key: str, default: Any = None) -> Any:
    if config is None or _is_mock(config) and not hasattr(config, "config"):
        return default
    if hasattr(config, "config") and config.config is not None:
        if hasattr(config.config, "extra") and isinstance(config.config.extra, dict):
            val = config.config.extra.get(key, None)
            if val is not None and not _is_mock(val):
                return val
        if hasattr(config.config, key):
            val = getattr(config.config, key)
            if val is not None and not _is_mock(val):
                return val
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "get") and not _is_mock(config):
        val = config.get(key, None)
        if val is not None and not _is_mock(val):
            return val
    val = getattr(config, key, None)
    if val is not None and not _is_mock(val):
        return val
    return default


def _get_column_name(cols_obj: Any, key: str) -> str:
    if not cols_obj or _is_mock(cols_obj):
        return key
    if isinstance(cols_obj, dict):
        val = cols_obj.get(key, key)
        return key if _is_mock(val) else str(val or key)
    val = getattr(cols_obj, key, key)
    return key if _is_mock(val) else str(val or key)


class TblPropertiesConfig(DatabricksComponentConfig):
    """Component encapsulating the tblproperties of a relation."""

    tblproperties: dict[str, str]
    pipeline_id: Optional[str] = None

    def get_diff(self, other: "TblPropertiesConfig") -> Optional["TblPropertiesConfig"]:
        # Asymmetric diff ignores server-managed props (present only on `other`); return the
        # full config, not the subset, since consumers render it as the create/refresh clause.
        if self.tblproperties.items() - other.tblproperties.items():
            return self
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
    def update_tblproperties_for_snapshot(
        cls,
        relation_config: Any,
        tblproperties: Optional[dict[str, Any]] = None,
        columns: Optional[list[str]] = None,
    ) -> dict[str, str]:
        if tblproperties is not None:
            result = {str(k): str(v) for k, v in tblproperties.items()}
        else:
            raw_props = _get_config_attr(relation_config, "tblproperties", {})
            if not isinstance(raw_props, dict):
                raise DbtRuntimeError("tblproperties must be a dictionary")
            result = {str(k): str(v) for k, v in raw_props.items()}

        custom_cols = _get_config_attr(relation_config, "snapshot_table_column_names") or {}
        scd_id = _get_column_name(custom_cols, "dbt_scd_id")
        updated_at = _get_column_name(custom_cols, "dbt_updated_at")
        valid_from = _get_column_name(custom_cols, "dbt_valid_from")
        valid_to = _get_column_name(custom_cols, "dbt_valid_to")

        meta_cols = [scd_id, updated_at, valid_from, valid_to]
        if _get_config_attr(relation_config, "hard_deletes") == "new_record":
            is_deleted = _get_column_name(custom_cols, "dbt_is_deleted")
            meta_cols.append(is_deleted)

        if "delta.dataSkippingStatsColumns" in result:
            existing_raw = str(result["delta.dataSkippingStatsColumns"])
            existing_cols = [c.strip() for c in existing_raw.split(",") if c.strip()]
            for col in meta_cols:
                if col not in existing_cols:
                    existing_cols.append(col)
            result["delta.dataSkippingStatsColumns"] = ", ".join(existing_cols)
        else:
            data_cols: list[str] = []
            if columns is not None:
                data_cols = [str(c) for c in columns if c not in meta_cols]
            else:
                raw_cols = None
                model = getattr(relation_config, "model", None)
                if model and not _is_mock(model):
                    raw_cols = (
                        model.get("columns")
                        if isinstance(model, dict)
                        else getattr(model, "columns", None)
                    )
                if not raw_cols:
                    cols_candidate = getattr(relation_config, "columns", None)
                    if not _is_mock(cols_candidate):
                        raw_cols = cols_candidate
                if not raw_cols:
                    raw_cols = _get_config_attr(relation_config, "columns")

                if isinstance(raw_cols, dict):
                    data_cols = [str(k) for k in raw_cols.keys() if k not in meta_cols]
                elif isinstance(raw_cols, (list, tuple)):
                    data_cols = [
                        str(c["name"] if isinstance(c, dict) else getattr(c, "name", c))
                        for c in raw_cols
                        if (c["name"] if isinstance(c, dict) else getattr(c, "name", c))
                        not in meta_cols
                    ]

            num_indexed_cols = 32
            if "delta.dataSkippingNumIndexedCols" in result:
                try:
                    num_indexed_cols = int(result["delta.dataSkippingNumIndexedCols"])
                except (ValueError, TypeError):
                    num_indexed_cols = 32

            leading_data_cols = data_cols[:num_indexed_cols]
            if leading_data_cols:
                final_cols = leading_data_cols + [
                    c for c in meta_cols if c not in leading_data_cols
                ]
            else:
                final_cols = meta_cols

            result["delta.dataSkippingStatsColumns"] = ", ".join(final_cols)

        return result

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

        materialized = base.get_config_value(relation_config, "materialized")
        file_format = (
            base.get_config_value(relation_config, "file_format") or constants.DELTA_FILE_FORMAT
        )
        is_delta = (
            table_format == constants.ICEBERG_TABLE_FORMAT and use_managed_iceberg is False
        ) or (
            table_format != constants.ICEBERG_TABLE_FORMAT
            and file_format == constants.DELTA_FILE_FORMAT
        )

        if materialized == "snapshot" and is_delta:
            tblproperties = cls.update_tblproperties_for_snapshot(relation_config, tblproperties)

        tblproperties = {str(k): str(v) for k, v in tblproperties.items()}
        return TblPropertiesConfig(tblproperties=tblproperties)
