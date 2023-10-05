from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.relation_configs.materialized_view import (
    DatabricksMaterializedViewConfig,
)


@pytest.mark.parametrize("bool_value", [True, False, "True", "False", "true", "false"])
def test_databricks_materialized_view_config_handles_all_valid_bools(bool_value):
    config = DatabricksMaterializedViewConfig(
        database_name="somedb",
        schema_name="public",
        mv_name="someview",
        query="select * from sometable",
    )
    model_node = Mock()
    model_node.config.extra.get = (
        lambda x, y=None: bool_value if x in ["auto_refresh", "backup"] else "someDistValue"
    )
    config_dict = config.parse_model_node(model_node)
    assert isinstance(config_dict["autorefresh"], bool)
    assert isinstance(config_dict["backup"], bool)


@pytest.mark.parametrize("bool_value", [1])
def test_databricks_materialized_view_config_throws_expected_exception_with_invalid_types(
    bool_value,
):
    config = DatabricksMaterializedViewConfig(
        database_name="somedb",
        schema_name="public",
        mv_name="someview",
        query="select * from sometable",
    )
    model_node = Mock()
    model_node.config.extra.get = (
        lambda x, y=None: bool_value if x in ["auto_refresh", "backup"] else "someDistValue"
    )
    with pytest.raises(TypeError):
        config.parse_model_node(model_node)


def test_databricks_materialized_view_config_throws_expected_exception_with_invalid_str():
    config = DatabricksMaterializedViewConfig(
        database_name="somedb",
        schema_name="public",
        mv_name="someview",
        query="select * from sometable",
    )
    model_node = Mock()
    model_node.config.extra.get = (
        lambda x, y=None: "notABool" if x in ["auto_refresh", "backup"] else "someDistValue"
    )
    with pytest.raises(ValueError):
        config.parse_model_node(model_node)
