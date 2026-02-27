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
        results = {
            "show_tblproperties": fixtures.gen_tblproperties(
                [["prop", "f1"], ["dbt.tblproperties.managedKeys", "prop"]]
            )
        }
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(
            tblproperties={"prop": "f1", "dbt.tblproperties.managedKeys": "prop"}
        )

    def test_from_results__multiple(self):
        results = {
            "show_tblproperties": fixtures.gen_tblproperties(
                [["prop", "1"], ["other", "other"], ["dbt.tblproperties.managedKeys", "other,prop"]]
            )
        }
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(
            tblproperties={
                "prop": "1",
                "other": "other",
                "dbt.tblproperties.managedKeys": "other,prop",
            }
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
        assert spec == TblPropertiesConfig(
            tblproperties={"prop": "1", "dbt.tblproperties.managedKeys": "prop"}
        )

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
                "dbt.tblproperties.managedKeys": ",".join(
                    [
                        "custom_prop",
                        "delta.enableIcebergCompatV2",
                        "delta.universalFormat.enabledFormats",
                    ]
                ),
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
        assert spec == TblPropertiesConfig(
            tblproperties={"custom_prop": "value", "dbt.tblproperties.managedKeys": "custom_prop"}
        )

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
