from agate import Table

from dbt.adapters.databricks.relation_configs.column_comments import ColumnCommentsConfig
from dbt.adapters.databricks.relation_configs.column_mask import ColumnMaskConfig
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.constraints import (
    CheckConstraint,
    ConstraintsConfig,
    ConstraintType,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from dbt.adapters.databricks.relation_configs.incremental import IncrementalTableConfig
from dbt.adapters.databricks.relation_configs.liquid_clustering import LiquidClusteringConfig
from dbt.adapters.databricks.relation_configs.row_filter import RowFilterConfig
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig


class TestIncrementalConfig:
    def test_from_results(self):
        results = {
            "information_schema.tags": Table(
                rows=[
                    ["tag1", "value1"],
                    ["tag2", "value2"],
                ],
                column_names=["tag_name", "tag_value"],
            ),
            "information_schema.column_tags": Table(
                rows=[
                    ["column", "sensitive", "true"],
                ],
                column_names=["column_name", "tag_name", "tag_value"],
            ),
            "show_tblproperties": Table(
                rows=[
                    ["prop", "f1"],
                    ["clusterByAuto", "true"],
                    ["clusteringColumns", '[["col1"],[""a""]]'],
                    ["delta.constraints.check_name_length", "LENGTH (name) >= 1"],
                ],
                column_names=["key", "value"],
            ),
            "describe_extended": Table(
                rows=[
                    ["column", "string", "test comment"],
                ],
                column_names=["col_name", "col_type", "comment"],
            ),
            "non_null_constraint_columns": Table(
                rows=[
                    ["id"],
                    ["email"],
                ],
                column_names=["column_name"],
            ),
            "primary_key_constraints": Table(
                rows=[
                    ["pk_user", "id"],
                    ["pk_user", "email"],
                ],
                column_names=["constraint_name", "column_name"],
            ),
            "foreign_key_constraints": Table(
                rows=[
                    ["fk_user_1", "id", "catalog", "schema", "customers", "customer_id"],
                    ["fk_user_1", "email", "catalog", "schema", "customers", "email"],
                    ["fk_user_2", "id", "catalog", "schema", "employees", "employee_id"],
                ],
                column_names=[
                    "constraint_name",
                    "from_column",
                    "to_catalog",
                    "to_schema",
                    "to_table",
                    "to_column",
                ],
            ),
            "column_masks": Table(
                rows=[["col1", "mask1", "col2"], ["col2", "mask2", "col1"]],
                column_names=["column_name", "mask_name", "using_columns"],
            ),
        }

        config = IncrementalTableConfig.from_results(results)

        assert config == IncrementalTableConfig(
            config={
                "comment": CommentConfig(comment=None, persist=False),
                "tags": TagsConfig(set_tags={"tag1": "value1", "tag2": "value2"}),
                "column_tags": ColumnTagsConfig(
                    set_column_tags={"column": {"sensitive": "True"}},
                ),
                "column_comments": ColumnCommentsConfig(
                    comments={"column": "test comment"}, quoted={}, persist=False
                ),
                "tblproperties": TblPropertiesConfig(
                    tblproperties={
                        "prop": "f1",
                        "delta.constraints.check_name_length": "LENGTH (name) >= 1",
                    }
                ),
                "liquid_clustering": LiquidClusteringConfig(
                    auto_cluster=True,
                    cluster_by=["col1", '"a"'],
                ),
                "constraints": ConstraintsConfig(
                    set_non_nulls=["id", "email"],
                    unset_non_nulls=[],
                    set_constraints=[
                        CheckConstraint(
                            type=ConstraintType.check,
                            name="check_name_length",
                            expression="LENGTH (name) >= 1",
                        ),
                        PrimaryKeyConstraint(
                            type=ConstraintType.primary_key,
                            name="pk_user",
                            columns=["id", "email"],
                        ),
                        ForeignKeyConstraint(
                            type=ConstraintType.foreign_key,
                            name="fk_user_1",
                            to="`catalog`.`schema`.`customers`",
                            to_columns=["customer_id", "email"],
                            columns=["id", "email"],
                        ),
                        ForeignKeyConstraint(
                            type=ConstraintType.foreign_key,
                            name="fk_user_2",
                            to="`catalog`.`schema`.`employees`",
                            to_columns=["employee_id"],
                            columns=["id"],
                        ),
                    ],
                    unset_constraints=[],
                ),
                "column_masks": ColumnMaskConfig(
                    set_column_masks={
                        "col1": {"function": "mask1", "using_columns": "col2"},
                        "col2": {"function": "mask2", "using_columns": "col1"},
                    },
                    unset_column_masks=[],
                ),
                "row_filter": RowFilterConfig(),
            }
        )
