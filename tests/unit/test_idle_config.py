import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks import context
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.contracts.graph import model_config
from dbt.contracts.graph.nodes import ModelNode


class TestDatabricksConnectionMaxIdleTime:
    """Test the various cases for determining a specified warehouse."""

    @pytest.fixture
    def errMsg(self):
        return (
            "Compute resource foo does not exist or does not specify http_path, "
            "relation: a_relation"
        )

    @pytest.fixture
    def creds(self):
        return DatabricksCredentials()

    @pytest.fixture
    def creds_with_idle(self, creds):
        creds.connect_max_idle = 77
        return creds

    @pytest.fixture
    def node(self):
        return ModelNode(
            relation_name="a_relation",
            database="database",
            schema="schema",
            name="node_name",
            resource_type="model",
            package_name="package",
            path="path",
            original_file_path="orig_path",
            unique_id="uniqueID",
            fqn=[],
            alias="alias",
            checksum=None,
        )

    @pytest.fixture
    def node_with_compute(self, node):
        node.config._extra = {"databricks_compute": "foo"}
        return node

    def test_get_max_idle__default(self, creds):
        # No node and nothing specified in creds
        time = context.get_max_idle_time(creds, None)
        assert context.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__with_no_node_config(self, creds, node):
        # node has no configuration so should get back default
        time = context.get_max_idle_time(creds, node)
        assert context.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__with_empty_config(self, creds, node):
        # empty configuration should return default
        node.config = model_config.ModelConfig()
        time = context.get_max_idle_time(creds, node)
        assert context.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__with_no_compute_config(self, creds, node_with_compute):
        # node that specifies a compute with no corresponding definition should return default
        time = context.get_max_idle_time(creds, node_with_compute)
        assert context.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__with_no_compute_in_creds(self, creds, node_with_compute):
        creds.compute = {}
        time = context.get_max_idle_time(creds, node_with_compute)
        assert context.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__with_compute_no_max_time(self, creds, node_with_compute):
        # if alternate compute doesn't specify a max time should return default
        creds.compute = {"foo": {}}
        time = context.get_max_idle_time(creds, node_with_compute)
        assert context.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__creds_with_max_idle(self, creds_with_idle):
        # No node so value should come from creds
        time = context.get_max_idle_time(creds_with_idle, None)
        assert creds_with_idle.connect_max_idle == time

    def test_get_max_idle__creds_with_max_idle_but_empty_node(self, creds_with_idle, node):
        # node has no configuration so should get value from creds
        time = context.get_max_idle_time(creds_with_idle, node)
        assert creds_with_idle.connect_max_idle == time

    def test_get_max_idle__creds_with_max_idle_but_no_compute_config(
        self, creds_with_idle, node_with_compute
    ):
        time = context.get_max_idle_time(creds_with_idle, node_with_compute)
        assert creds_with_idle.connect_max_idle == time

    def test_get_max_idle__creds_with_max_idle_and_compute_config_but_no_value(
        self, creds_with_idle, node_with_compute
    ):
        # if alternate compute doesn't specify a max time should get value from creds
        creds_with_idle.compute = {"foo": {}}
        time = context.get_max_idle_time(creds_with_idle, node_with_compute)
        assert creds_with_idle.connect_max_idle == time

    def test_get_max_idle__golden_path(self, creds_with_idle, node_with_compute):
        compute_idle_time = 88
        creds_with_idle.compute = {"foo": {"connect_max_idle": compute_idle_time}}

        time = context.get_max_idle_time(creds_with_idle, node_with_compute)
        assert compute_idle_time == time

    def test_get_max_idle__invalid(self, creds_with_idle, node_with_compute):
        creds_with_idle.compute = {"foo": {"connect_max_idle": "foo"}}

        with pytest.raises(DbtRuntimeError, match="foo is not a valid value"):
            context.get_max_idle_time(creds_with_idle, node_with_compute)

    def test_get_max_idle__with_string(self, creds_with_idle, node_with_compute):
        creds_with_idle.compute = {"foo": {"connect_max_idle": "99"}}

        time = context.get_max_idle_time(creds_with_idle, node_with_compute)
        assert time == 99
