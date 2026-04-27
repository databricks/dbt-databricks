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


class TestTblPropertiesConfig:
    def test_get_diff__empty_and_some_exist(self):
        # tblproperties are "set only" - when config has no tblproperties and relation has tblproperties,
        # we don't unset the existing tblproperties
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
        # tblproperties are "set only" - only the new/updated tblproperties are included
        config_properties = TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})
        relation_properties = TblPropertiesConfig(tblproperties={"prop": "2", "c": "value"})
        diff = config_properties.get_diff(relation_properties)
        assert diff == TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})

    def test_get_diff__no_changes(self):
        config_properties = TblPropertiesConfig(tblproperties={"prop": "1"})
        relation_properties = TblPropertiesConfig(tblproperties={"prop": "1"})
        diff = config_properties.get_diff(relation_properties)
        assert diff is None

