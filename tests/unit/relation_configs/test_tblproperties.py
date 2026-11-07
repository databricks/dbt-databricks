from unittest.mock import Mock

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.global_state import GlobalState
from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesConfig,
    TblPropertiesProcessor,
)
from tests.unit import fixtures


class TestTblPropertiesProcessor:
    def test_from_results__none(self):
        results = {"show_tblproperties": None}
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(tblproperties={})

    def test_from_results__single(self):
        results = {"show_tblproperties": fixtures.gen_tblproperties([["prop", "f1"]])}
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "f1"})

    def test_from_results__multiple(self):
        results = {
            "show_tblproperties": fixtures.gen_tblproperties([["prop", "1"], ["other", "other"]])
        }
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})

    def test_from_results__retains_server_set_properties(self):
        # Server-set properties are kept here; get_diff filters them out, not from_relation_results.
        results = {
            "show_tblproperties": fixtures.gen_tblproperties(
                [["prop", "1"], ["delta.parquet.compression.codec", "zstd"]]
            )
        }
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(
            tblproperties={"prop": "1", "delta.parquet.compression.codec": "zstd"}
        )

    def test_from_model_node__without_tblproperties(self):
        model = Mock()
        model.config.extra = {}
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(tblproperties={})

    def test_from_model_node__with_tblproperties(self):
        model = Mock()
        model.config.extra = {
            "tblproperties": {"prop": 1},
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "1"})

    def test_from_model_node__with_empty_tblproperties(self):
        model = Mock()
        model.config.extra = {"tblproperties": {}}
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(tblproperties={})

    def test_from_model_node__with_incorrect_tblproperties(self):
        model = Mock()
        model.config.extra = {"tblproperties": True}
        with pytest.raises(
            DbtRuntimeError,
            match="tblproperties must be a dictionary",
        ):
            _ = TblPropertiesProcessor.from_relation_config(model)

    def test_from_model_node__with_uniform_iceberg_adds_properties(self):
        GlobalState.set_use_managed_iceberg(False)
        model = Mock()
        model.config.extra = {
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "tblproperties": {"custom_prop": "value"},
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        # Should have both custom property AND UniForm properties
        assert spec == TblPropertiesConfig(
            tblproperties={
                "custom_prop": "value",
                "delta.enableIcebergCompatV2": "true",
                "delta.universalFormat.enabledFormats": constants.ICEBERG_TABLE_FORMAT,
            }
        )

    def test_from_model_node__with_managed_iceberg_no_uniform_properties(self):
        GlobalState.set_use_managed_iceberg(True)
        model = Mock()
        model.config.extra = {
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "tblproperties": {"custom_prop": "value"},
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        # Should only have the custom property, NOT the UniForm properties
        assert spec == TblPropertiesConfig(tblproperties={"custom_prop": "value"})

    def test_from_model_node__with_iceberg_no_flag_no_properties(self):
        GlobalState.set_use_managed_iceberg(None)
        model = Mock()
        model.config.extra = {
            "table_format": constants.ICEBERG_TABLE_FORMAT,
            "tblproperties": {},
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        # Should not have UniForm properties without explicit use_managed_iceberg=False
        assert spec == TblPropertiesConfig(tblproperties={})

    def test_from_model_node__snapshot_default_columns(self):
        model = Mock()
        model.config.extra = {
            "materialized": "snapshot",
            "file_format": constants.DELTA_FILE_FORMAT,
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(
            tblproperties={
                "delta.dataSkippingStatsColumns": (
                    "dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to"
                )
            }
        )

    def test_from_model_node__snapshot_with_data_columns(self):
        model = Mock()
        data_cols = [f"c{i}" for i in range(1, 41)]
        model.columns = {c: {} for c in data_cols}
        model.config.extra = {
            "materialized": "snapshot",
            "file_format": constants.DELTA_FILE_FORMAT,
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        # Leading 32 columns + 4 snapshot meta columns
        expected_cols = [f"c{i}" for i in range(1, 33)] + [
            "dbt_scd_id",
            "dbt_updated_at",
            "dbt_valid_from",
            "dbt_valid_to",
        ]
        assert spec == TblPropertiesConfig(
            tblproperties={"delta.dataSkippingStatsColumns": ", ".join(expected_cols)}
        )

    def test_from_model_node__snapshot_respects_num_indexed_cols(self):
        model = Mock()
        data_cols = [f"c{i}" for i in range(1, 20)]
        model.columns = {c: {} for c in data_cols}
        model.config.extra = {
            "materialized": "snapshot",
            "file_format": constants.DELTA_FILE_FORMAT,
            "tblproperties": {"delta.dataSkippingNumIndexedCols": "5"},
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        expected_cols = [f"c{i}" for i in range(1, 6)] + [
            "dbt_scd_id",
            "dbt_updated_at",
            "dbt_valid_from",
            "dbt_valid_to",
        ]
        assert spec == TblPropertiesConfig(
            tblproperties={
                "delta.dataSkippingNumIndexedCols": "5",
                "delta.dataSkippingStatsColumns": ", ".join(expected_cols),
            }
        )

    def test_from_model_node__snapshot_custom_column_names(self):
        model = Mock()
        model.config.extra = {
            "materialized": "snapshot",
            "file_format": constants.DELTA_FILE_FORMAT,
            "snapshot_table_column_names": {
                "dbt_scd_id": "_scd_id",
                "dbt_updated_at": "_updated_at",
                "dbt_valid_from": "start_date",
                "dbt_valid_to": "end_date",
                "dbt_is_deleted": "is_deleted",
            },
            "hard_deletes": "new_record",
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(
            tblproperties={
                "delta.dataSkippingStatsColumns": (
                    "_scd_id, _updated_at, start_date, end_date, is_deleted"
                )
            }
        )

    def test_from_model_node__snapshot_hard_deletes_new_record(self):
        model = Mock()
        model.config.extra = {
            "materialized": "snapshot",
            "file_format": constants.DELTA_FILE_FORMAT,
            "hard_deletes": "new_record",
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(
            tblproperties={
                "delta.dataSkippingStatsColumns": (
                    "dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to, dbt_is_deleted"
                )
            }
        )

    def test_from_model_node__snapshot_supplements_user_data_skipping_columns(self):
        model = Mock()
        model.config.extra = {
            "materialized": "snapshot",
            "file_format": constants.DELTA_FILE_FORMAT,
            "tblproperties": {
                "delta.dataSkippingStatsColumns": "dbt_valid_to, custom_col1, custom_col2",
                "custom_prop": "val",
            },
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(
            tblproperties={
                "custom_prop": "val",
                "delta.dataSkippingStatsColumns": (
                    "dbt_valid_to, custom_col1, custom_col2, "
                    "dbt_scd_id, dbt_updated_at, dbt_valid_from"
                ),
            }
        )

    def test_from_model_node__snapshot_non_delta_ignored(self):
        model = Mock()
        model.config.extra = {
            "materialized": "snapshot",
            "file_format": constants.HUDI_FILE_FORMAT,
            "tblproperties": {"custom_prop": "val"},
        }
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(tblproperties={"custom_prop": "val"})


class TestTblPropertiesConfig:
    def test_get_diff__empty_and_some_exist(self):
        # tblproperties are "set only" - when config has no tblproperties and the relation
        # has some, we don't unset the existing tblproperties
        config_properties = TblPropertiesConfig(tblproperties={})
        relation_properties = TblPropertiesConfig(tblproperties={"prop": "1"})
        diff = config_properties.get_diff(relation_properties)
        assert diff is None  # No changes needed since we don't unset tblproperties

    def test_get_diff__some_new_and_empty_existing(self):
        config_properties = TblPropertiesConfig(tblproperties={"prop": "1"})
        relation_properties = TblPropertiesConfig(tblproperties={})
        diff = config_properties.get_diff(relation_properties)
        assert diff == TblPropertiesConfig(tblproperties={"prop": "1"})

    def test_get_diff__mixed_case(self):
        # Both desired props differ from the relation, so get_diff returns the full config.
        config_properties = TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})
        relation_properties = TblPropertiesConfig(tblproperties={"prop": "2", "c": "value"})
        diff = config_properties.get_diff(relation_properties)
        assert diff == TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})

    def test_get_diff__retains_already_applied_properties(self):
        # "a" is already applied and only "c" is new, but get_diff must return the full desired
        # config so create/refresh rendering doesn't drop already-applied properties.
        config_properties = TblPropertiesConfig(tblproperties={"a": "1", "c": "1"})
        relation_properties = TblPropertiesConfig(tblproperties={"a": "1"})
        diff = config_properties.get_diff(relation_properties)
        assert diff == TblPropertiesConfig(tblproperties={"a": "1", "c": "1"})

    def test_get_diff__no_changes(self):
        config_properties = TblPropertiesConfig(tblproperties={"prop": "1"})
        relation_properties = TblPropertiesConfig(tblproperties={"prop": "1"})
        diff = config_properties.get_diff(relation_properties)
        assert diff is None

    def test_get_diff__ignores_server_set_properties(self):
        # A server-set property (e.g. the default parquet compression codec) present on the
        # relation but absent from the model config must not register as a change.
        config_properties = TblPropertiesConfig(tblproperties={"prop": "1"})
        relation_properties = TblPropertiesConfig(
            tblproperties={"prop": "1", "delta.parquet.compression.codec": "zstd"}
        )
        diff = config_properties.get_diff(relation_properties)
        assert diff is None
