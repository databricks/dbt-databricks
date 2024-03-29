import unittest

import dbt.exceptions
from dbt.adapters.databricks import connections
from dbt.contracts.graph import model_config
from dbt.contracts.graph import nodes


class TestDatabricksConnectionMaxIdleTime(unittest.TestCase):
    """Test the various cases for determining a specified warehouse."""

    errMsg = (
        "Compute resource foo does not exist or does not specify http_path, " "relation: a_relation"
    )

    def test_get_max_idle_default(self):
        creds = connections.DatabricksCredentials()

        # No node and nothing specified in creds
        time = connections._get_max_idle_time(None, creds)
        self.assertEqual(connections.DEFAULT_MAX_IDLE_TIME, time)

        node = nodes.ModelNode(
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

        # node has no configuration so should get back default
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(connections.DEFAULT_MAX_IDLE_TIME, time)

        # empty configuration should return default
        node.config = model_config.ModelConfig()
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(connections.DEFAULT_MAX_IDLE_TIME, time)

        # node with no extras in configuration should return default
        node.config._extra = {}
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(connections.DEFAULT_MAX_IDLE_TIME, time)

        # node that specifies a compute with no corresponding definition should return default
        node.config._extra["databricks_compute"] = "foo"
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(connections.DEFAULT_MAX_IDLE_TIME, time)

        creds.compute = {}
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(connections.DEFAULT_MAX_IDLE_TIME, time)

        # if alternate compute doesn't specify a max time should return default
        creds.compute = {"foo": {}}
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(connections.DEFAULT_MAX_IDLE_TIME, time)
        # with self.assertRaisesRegex(
        #     dbt.exceptions.DbtRuntimeError,
        #     self.errMsg,
        # ):
        #     connections._get_http_path(node, creds)

        # creds.compute = {"foo": {"http_path": "alternate_path"}}
        # path = connections._get_http_path(node, creds)
        # self.assertEqual("alternate_path", path)

    def test_get_max_idle_creds(self):
        creds_idle_time = 77
        creds = connections.DatabricksCredentials(connect_max_idle=creds_idle_time)

        # No node so value should come from creds
        time = connections._get_max_idle_time(None, creds)
        self.assertEqual(creds_idle_time, time)

        node = nodes.ModelNode(
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

        # node has no configuration so should get value from creds
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(creds_idle_time, time)

        # empty configuration should get value from creds
        node.config = model_config.ModelConfig()
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(creds_idle_time, time)

        # node with no extras in configuration should get value from creds
        node.config._extra = {}
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(creds_idle_time, time)

        # node that specifies a compute with no corresponding definition should get value from creds
        node.config._extra["databricks_compute"] = "foo"
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(creds_idle_time, time)

        creds.compute = {}
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(creds_idle_time, time)

        # if alternate compute doesn't specify a max time should get value from creds
        creds.compute = {"foo": {}}
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(creds_idle_time, time)

    def test_get_max_idle_compute(self):
        creds_idle_time = 88
        compute_idle_time = 77
        creds = connections.DatabricksCredentials(connect_max_idle=creds_idle_time)
        creds.compute = {"foo": {"connect_max_idle": compute_idle_time}}

        node = nodes.SnapshotNode(
            config=None,
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

        node.config = model_config.SnapshotConfig()
        node.config._extra = {"databricks_compute": "foo"}

        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(compute_idle_time, time)

    def test_get_max_idle_invalid(self):
        creds_idle_time = "foo"
        compute_idle_time = "bar"
        creds = connections.DatabricksCredentials(connect_max_idle=creds_idle_time)
        creds.compute = {"alternate_compute": {"connect_max_idle": compute_idle_time}}

        node = nodes.SnapshotNode(
            config=None,
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

        node.config = model_config.SnapshotConfig()

        with self.assertRaisesRegex(
            dbt.exceptions.DbtRuntimeError,
            f"{creds_idle_time} is not a valid value for connect_max_idle. "
            "Must be a number of seconds.",
        ):
            connections._get_max_idle_time(node, creds)

        node.config._extra["databricks_compute"] = "alternate_compute"
        with self.assertRaisesRegex(
            dbt.exceptions.DbtRuntimeError,
            f"{compute_idle_time} is not a valid value for connect_max_idle. "
            "Must be a number of seconds.",
        ):
            connections._get_max_idle_time(node, creds)

        creds.compute["alternate_compute"]["connect_max_idle"] = "1.2.3"
        with self.assertRaisesRegex(
            dbt.exceptions.DbtRuntimeError,
            "1.2.3 is not a valid value for connect_max_idle. " "Must be a number of seconds.",
        ):
            connections._get_max_idle_time(node, creds)

        creds.compute["alternate_compute"]["connect_max_idle"] = "1,002.3"
        with self.assertRaisesRegex(
            dbt.exceptions.DbtRuntimeError,
            "1,002.3 is not a valid value for connect_max_idle. " "Must be a number of seconds.",
        ):
            connections._get_max_idle_time(node, creds)

    def test_get_max_idle_simple_string_conversion(self):
        creds_idle_time = "12"
        compute_idle_time = "34"
        creds = connections.DatabricksCredentials(connect_max_idle=creds_idle_time)
        creds.compute = {"alternate_compute": {"connect_max_idle": compute_idle_time}}

        node = nodes.SnapshotNode(
            config=None,
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

        node.config = model_config.SnapshotConfig()

        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(float(creds_idle_time), time)

        node.config._extra["databricks_compute"] = "alternate_compute"
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(float(compute_idle_time), time)

        creds.compute["alternate_compute"]["connect_max_idle"] = "  56 "
        time = connections._get_max_idle_time(node, creds)
        self.assertEqual(56, time)
