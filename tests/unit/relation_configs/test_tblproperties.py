import pytest
from agate import Table
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesProcessor,
)
from dbt.exceptions import DbtRuntimeError
from mock import Mock


class TestTblPropertiesProcessor:
    def test_from_results__none(self):
        results = {"show_tblproperties": None}
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(tblproperties={})

    def test_from_results__single(self):
        results = {"show_tblproperties": Table(rows=[["prop", "f1"]])}
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "f1"})

    def test_from_results__multiple(self):
        results = {"show_tblproperties": Table(rows=[["prop", "1"], ["other", "other"]])}
        spec = TblPropertiesProcessor.from_relation_results(results)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})

    def test_from_model_node__without_tblproperties(self):
        model = Mock()
        model.config.extra = {}
        spec = TblPropertiesProcessor.from_relation_config(model)
        assert spec == TblPropertiesConfig(tblproperties={})

    def test_from_model_node__with_tblpropoerties(self):
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
