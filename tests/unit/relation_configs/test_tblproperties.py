from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesConfig,
    TblPropertiesProcessor,
)
from dbt.exceptions import DbtRuntimeError
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
