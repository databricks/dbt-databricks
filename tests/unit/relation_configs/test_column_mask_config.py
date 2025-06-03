from unittest.mock import Mock

from agate import Table

from dbt.adapters.databricks.relation_configs.column_mask import (
    ColumnMaskConfig,
    ColumnMaskProcessor,
)


class TestColumnMaskProcessor:
    def test_from_relation_results__none(self):
        results = {
            "column_masks": Table(
                rows=[], column_names=["column_name", "mask_name", "using_columns"]
            )
        }
        spec = ColumnMaskProcessor.from_relation_results(results)
        assert spec == ColumnMaskConfig(set_column_masks={})

    def test_from_relation_results__some(self):
        results = {
            "column_masks": Table(
                rows=[
                    ["col1", "mask1", None],
                    ["col2", "mask2", "col1"],
                ],
                column_names=["column_name", "mask_name", "using_columns"],
            )
        }
        spec = ColumnMaskProcessor.from_relation_results(results)
        assert spec == ColumnMaskConfig(
            set_column_masks={
                "col1": {"function": "mask1"},
                "col2": {"function": "mask2", "using_columns": "col1"},
            }
        )

    def test_from_relation_config__none(self):
        model = Mock()
        model.columns = {}
        spec = ColumnMaskProcessor.from_relation_config(model)
        assert spec == ColumnMaskConfig(set_column_masks={})

    def test_from_relation_config__with_masks(self):
        model = Mock()
        model.database = "db"
        model.schema = "schema"
        model.columns = {
            "col1": {"_extra": {"column_mask": {"function": "mask1"}}},
            "col2": {"_extra": {"column_mask": {"function": "mask2", "using_columns": "col3"}}},
            "col3": {"_extra": {}},
        }
        spec = ColumnMaskProcessor.from_relation_config(model)
        assert spec == ColumnMaskConfig(
            set_column_masks={
                "col1": {"function": "db.schema.mask1"},
                "col2": {"function": "db.schema.mask2", "using_columns": "col3"},
            }
        )


class TestColumnMaskConfig:
    def test_get_diff__empty_and_some_exist(self):
        config = ColumnMaskConfig(set_column_masks={})
        other = ColumnMaskConfig(set_column_masks={"col1": {"function": "mask1"}})
        diff = config.get_diff(other)
        assert diff == ColumnMaskConfig(set_column_masks={}, unset_column_masks=["col1"])

    def test_get_diff__some_new_and_empty_existing(self):
        config = ColumnMaskConfig(set_column_masks={"col1": {"function": "mask1"}})
        other = ColumnMaskConfig(set_column_masks={})
        diff = config.get_diff(other)
        assert diff == ColumnMaskConfig(
            set_column_masks={"col1": {"function": "mask1"}}, unset_column_masks=[]
        )

    def test_get_diff__mixed_case(self):
        config = ColumnMaskConfig(
            set_column_masks={
                "col1": {"function": "mask1"},
                "col2": {"function": "mask2"},
            }
        )
        other = ColumnMaskConfig(
            set_column_masks={
                "col2": {"function": "mask3"},
                "col3": {"function": "mask4"},
            }
        )
        diff = config.get_diff(other)
        assert diff == ColumnMaskConfig(
            set_column_masks={
                "col1": {"function": "mask1"},
                "col2": {"function": "mask2"},
            },
            unset_column_masks=["col3"],
        )

    def test_get_diff__no_changes(self):
        config = ColumnMaskConfig(set_column_masks={"col1": {"function": "mask1"}})
        other = ColumnMaskConfig(set_column_masks={"col1": {"function": "mask1"}})
        diff = config.get_diff(other)
        assert diff is None
