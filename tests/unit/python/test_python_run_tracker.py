from mock import Mock

from dbt.adapters.databricks.python_models.run_tracking import PythonRunTracker


class TestPythonRunTracker:
    def test_cancel_runs__from_separate_instance(self):
        tracker = PythonRunTracker()
        tracker.insert_run_id("run_id")
        mock_client = Mock()

        PythonRunTracker.cancel_runs(mock_client)
        mock_client.job_runs.cancel.assert_called_once_with("run_id")

    def test_cancel_runs__with_command_execution(self):
        tracker = PythonRunTracker()
        mock_command = Mock()
        tracker.insert_command(mock_command)
        mock_client = Mock()

        PythonRunTracker.cancel_runs(mock_client)
        mock_client.commands.cancel.assert_called_once_with(mock_command)
