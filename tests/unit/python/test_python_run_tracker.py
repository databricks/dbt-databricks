from dbt.adapters.databricks.python_submissions import PythonRunTracker
from mock import Mock


class TestPythonRunTracker:
    def test_cancel_runs__from_separate_instance(self):
        tracker = PythonRunTracker()
        tracker.set_host("host")
        tracker.insert_run_id("run_id")
        other_tracker = PythonRunTracker()
        mock_session = Mock()

        other_tracker.cancel_runs(mock_session)
        mock_session.post.assert_called_once_with(
            "https://host/api/2.1/jobs/runs/cancel",
            json={"run_id": "run_id"},
        )
