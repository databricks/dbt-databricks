import unittest
from unittest.mock import Mock

from mock import PropertyMock
from dbt.adapters.databricks.connections import DatabricksCredentials

from dbt.adapters.databricks.python_submissions import DBContext, BaseDatabricksHelper


class TestDatabricksPythonSubmissions(unittest.TestCase):
    def test_start_cluster_returns_on_receiving_running_state(self):
        session_mock = Mock()
        # Mock the start command
        post_mock = Mock()
        post_mock.status_code = 200
        session_mock.post.return_value = post_mock
        # Mock the status command
        get_mock = Mock()
        get_mock.status_code = 200
        get_mock.json.return_value = {"state": "RUNNING"}
        session_mock.get.return_value = get_mock

        context = DBContext(Mock(), None, None, session_mock)
        context.start_cluster()

        session_mock.get.assert_called_once()


class DatabricksTestHelper(BaseDatabricksHelper):
    def __init__(self, parsed_model: dict, credentials: DatabricksCredentials):
        self.parsed_model = parsed_model
        self.credentials = credentials


class TestAclUpdate:
    def test_empty_acl_empty_config(self):
        helper = DatabricksTestHelper({"config": {}}, DatabricksCredentials())
        assert helper._update_with_acls({}) == {}

    def test_empty_acl_non_empty_config(self):
        helper = DatabricksTestHelper({"config": {}}, DatabricksCredentials())
        assert helper._update_with_acls({"a": "b"}) == {"a": "b"}

    def test_non_empty_acl_empty_config(self):
        expected_access_control = {
            "access_control_list": [
                {"user_name": "user2", "permission_level": "CAN_VIEW"},
            ]
        }
        helper = DatabricksTestHelper({"config": expected_access_control}, DatabricksCredentials())
        assert helper._update_with_acls({}) == expected_access_control

    def test_non_empty_acl_non_empty_config(self):
        expected_access_control = {
            "access_control_list": [
                {"user_name": "user2", "permission_level": "CAN_VIEW"},
            ]
        }
        helper = DatabricksTestHelper({"config": expected_access_control}, DatabricksCredentials())
        assert helper._update_with_acls({"a": "b"}) == {
            "a": "b",
            "access_control_list": expected_access_control["access_control_list"],
        }
