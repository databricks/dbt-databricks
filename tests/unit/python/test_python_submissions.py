import unittest
from unittest.mock import patch, Mock

from dbt.adapters.databricks.python_submissions import DBContext


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
