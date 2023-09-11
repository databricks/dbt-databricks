import unittest
from unittest.mock import patch, Mock, MagicMock

from dbt.adapters.databricks.python_submissions import DBContext, BaseDatabricksHelper


class TestDatabricksPythonSubmissions(unittest.TestCase):
    """
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
    """

    @patch("requests.get")
    @patch("requests.post")
    def test_submit_job_logging(self, mock_post, mock_get):
        log_prefix = "Submitted databricks job: "
        log_dict = {"run_id": "1"}
        logger_name = "Databricks"

        with self.assertLogs("stdout", level='INFO') as cm:
            # Mock the start command
            mock_post.return_value.status_code = 200
            # Mock the status command
            mock_get.return_value.status_code = 200
            mock_get.return_value.json = Mock(return_value=log_dict)

            with patch.object(BaseDatabricksHelper, "__init__", lambda x, y, z: None):
                job_helper = BaseDatabricksHelper(Mock(), Mock())
                job_helper.schema = "schema"
                job_helper.identifier = "identifier"
                job_helper.parsed_model = {}
                job_helper.parsed_model["config"] = {}
                job_helper.credentials = Mock()
                job_helper.auth_header = Mock()
                job_helper._submit_job("/test/path", {})

        expected_log = f"INFO:{logger_name}:{log_prefix}{log_dict}"
        print("HEYO: {cm.output}")
        assert expected_log in cm.output