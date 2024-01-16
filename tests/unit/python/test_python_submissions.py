import unittest
from unittest.mock import patch, Mock

from dbt.adapters.databricks.connections import DatabricksCredentials

from dbt.adapters.databricks.python_submissions import DBContext, BaseDatabricksHelper


class TestDatabricksPythonSubmissions(unittest.TestCase):
    @patch("requests.get")
    @patch("requests.post")
    def test_start_cluster_returns_on_receiving_running_state(self, mock_post, mock_get):
        # Mock the start command
        mock_post.return_value.status_code = 200
        # Mock the status command
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = Mock(return_value={"state": "RUNNING"})

        context = DBContext(Mock(), None, None)
        context.start_cluster()

        mock_get.assert_called_once()


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
