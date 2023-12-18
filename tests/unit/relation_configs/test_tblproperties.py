from mock import Mock
import pytest
from agate import Table

from dbt.adapters.databricks.relation_configs.tblproperties import (
    TblPropertiesConfig,
    TblPropertiesProcessor,
)
from dbt.exceptions import DbtRuntimeError


class TestTblPropertiesConfig:
    def test_post_init__conflicting_tblproperties(self):
        with pytest.raises(
            DbtRuntimeError,
            match="Cannot set and unset the same tblproperty in the same model",
        ):
            config = TblPropertiesConfig({"prop": "1"}, to_unset=["prop"])

    @pytest.mark.parametrize(
        "input,expected",
        [
            (None, ""),
            (dict(), ""),
            ({"prop": 1}, "TBLPROPERTIES ('prop' = '1')"),
            ({"prop": 1, "other": "thing"}, "TBLPROPERTIES ('prop' = '1', 'other' = 'thing')"),
        ],
    )
    def test_to_sql_clause(self, input, expected):
        config = TblPropertiesConfig(tblproperties=input)
        assert config.to_sql_clause() == expected

    def test_eq__match(self):
        config1 = TblPropertiesConfig(tblproperties={"prop": "1"})
        config2 = TblPropertiesConfig(tblproperties={"prop": "1"})
        assert config1 == config2

    def test_eq__mismatch(self):
        config1 = TblPropertiesConfig(tblproperties={"prop": "1"})
        config2 = TblPropertiesConfig(tblproperties={"prop": "2"})
        assert config1 != config2

    def test_eq__ignore_list(self):
        config1 = TblPropertiesConfig(tblproperties={"prop": "1"}, ignore_list=["prop"])
        config2 = TblPropertiesConfig(tblproperties={"prop": "2"})
        assert config1 == config2

    def test_eq__ignore_list_extra(self):
        config1 = TblPropertiesConfig(
            tblproperties={"prop": "1", "other": "other"}, ignore_list=["prop"]
        )
        config2 = TblPropertiesConfig(tblproperties={"prop": "2", "other": "other"})
        assert config1 == config2

    def test_eq__default_ignore_list(self):
        config1 = TblPropertiesConfig(tblproperties={"pipelines.pipelineId": "1"})
        config2 = TblPropertiesConfig(tblproperties={})
        assert config1 == config2

    def test_get_diff__same_properties(self):
        config = TblPropertiesConfig(tblproperties={"prop": "1"})
        diff = config.get_diff(config)
        assert diff == TblPropertiesConfig()

    def test_get_diff__other_has_added_prop(self):
        config = TblPropertiesConfig(tblproperties={"prop": "1"})
        other = TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})
        diff = config.get_diff(other)
        assert diff == TblPropertiesConfig(to_unset=["other"])

    def test_get_diff__other_has_diff_value(self):
        config = TblPropertiesConfig(tblproperties={"prop": "1"})
        other = TblPropertiesConfig(tblproperties={"prop": "2"})
        diff = config.get_diff(other)
        assert diff == TblPropertiesConfig(tblproperties={"prop": "1"})

    def test_get_diff__mix(self):
        config = TblPropertiesConfig(
            tblproperties={"prop": "1", "other": "2", "third": "3"}, ignore_list=["third"]
        )
        other = TblPropertiesConfig(
            tblproperties={"prop": "2", "other": "2", "third": "4", "fourth": "5"}
        )
        diff = config.get_diff(other)
        assert diff == TblPropertiesConfig(tblproperties={"prop": "1"}, to_unset=["fourth"])

    def test_to_alter_sql_clauses__none(self):
        config = TblPropertiesConfig()
        assert config.to_alter_sql_clauses() == []

    def test_to_alter_sql_clauses__set_only(self):
        config = TblPropertiesConfig({"prop": "1", "other": "other"})
        assert config.to_alter_sql_clauses() == [
            "SET TBLPROPERTIES ('prop' = '1', 'other' = 'other')"
        ]

    def test_to_alter_sql_clauses__unset_only(self):
        config = TblPropertiesConfig(to_unset=["prop", "other"])
        assert config.to_alter_sql_clauses() == ["UNSET TBLPROPERTIES IF EXISTS ('prop', 'other')"]

    def test_to_alter_sql_clauses__both(self):
        config = TblPropertiesConfig(
            {"prop": "1"},
            to_unset=["other"],
        )
        assert config.to_alter_sql_clauses() == [
            "SET TBLPROPERTIES ('prop' = '1')",
            "UNSET TBLPROPERTIES IF EXISTS ('other')",
        ]


class TestTblPropertiesProcessor:
    def test_from_results__none(self):
        results = {"show_tblproperties": None}
        spec = TblPropertiesProcessor.from_results(results)
        assert spec == TblPropertiesConfig()

    def test_from_results__single(self):
        results = {"show_tblproperties": Table(rows=[["prop", "f1"]])}
        spec = TblPropertiesProcessor.from_results(results)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "f1"})

    def test_from_results__multiple(self):
        results = {"show_tblproperties": Table(rows=[["prop", "1"], ["other", "other"]])}
        spec = TblPropertiesProcessor.from_results(results)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "1", "other": "other"})

    def test_from_model_node__without_tblproperties_config(self):
        model = Mock()
        model.config.extra = {}
        spec = TblPropertiesProcessor.from_model_node(model)
        assert spec == TblPropertiesConfig()

    def test_from_model_node__with_all_config(self):
        model = Mock()
        model.config.extra = {
            "tblproperties_config": {
                "tblproperties": {"prop": 1},
                "ignore_list": ["other"],
            }
        }
        spec = TblPropertiesProcessor.from_model_node(model)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "1"}, ignore_list=["other"])

    def test_from_model_node__without_ignore_list(self):
        model = Mock()
        model.config.extra = {
            "tblproperties_config": {
                "tblproperties": {"prop": 1},
            }
        }
        spec = TblPropertiesProcessor.from_model_node(model)
        assert spec == TblPropertiesConfig(tblproperties={"prop": "1"})

    def test_from_model_node__without_tblproperties(self):
        model = Mock()
        model.config.extra = {
            "tblproperties_config": {
                "ignore_list": ["other"],
            }
        }
        spec = TblPropertiesProcessor.from_model_node(model)
        assert spec == TblPropertiesConfig(ignore_list=["other"])

    def test_from_model_node__with_incorrect_tblproperties(self):
        model = Mock()
        model.config.extra = {
            "tblproperties_config": {"ignore_list": ["other"], "tblproperties": True}
        }
        with pytest.raises(
            DbtRuntimeError,
            match="tblproperties must be a dictionary",
        ):
            TblPropertiesProcessor.from_model_node(model)

    def test_from_model_node__with_incorrect_ignore_list(self):
        model = Mock()
        model.config.extra = {
            "tblproperties_config": {"tblproperties": {"prop": 1}, "ignore_list": True}
        }
        with pytest.raises(
            DbtRuntimeError,
            match="ignore_list must be a list",
        ):
            TblPropertiesProcessor.from_model_node(model)
