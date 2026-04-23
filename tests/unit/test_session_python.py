"""Unit tests for session mode Python model submission."""

from unittest.mock import MagicMock, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.python_models.python_submissions import (
    SessionPythonJobHelper,
    SessionPythonSubmitter,
    SessionStateManager,
)


class TestSessionStateManager:
    """Tests for SessionStateManager."""

    def test_get_clean_exec_globals_includes_spark(self):
        """Test that get_clean_exec_globals includes spark."""
        mock_spark = MagicMock()

        globals_dict = SessionStateManager.get_clean_exec_globals(mock_spark)

        assert globals_dict["spark"] is mock_spark

    def test_get_clean_exec_globals_includes_dbt(self):
        """Test that get_clean_exec_globals includes dbt module."""
        mock_spark = MagicMock()

        globals_dict = SessionStateManager.get_clean_exec_globals(mock_spark)

        assert "dbt" in globals_dict

    def test_cleanup_temp_views_drops_temp_views(self):
        """Test that cleanup_temp_views drops temporary views."""
        mock_spark = MagicMock()
        mock_row = MagicMock()
        mock_row.viewName = "temp_view_1"
        mock_row.isTemporary = True
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        SessionStateManager.cleanup_temp_views(mock_spark)

        mock_spark.catalog.dropTempView.assert_called_once_with("temp_view_1")

    def test_cleanup_temp_views_ignores_non_temp_views(self):
        """Test that cleanup_temp_views ignores non-temporary views."""
        mock_spark = MagicMock()
        mock_row = MagicMock()
        mock_row.viewName = "permanent_view"
        mock_row.isTemporary = False
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        SessionStateManager.cleanup_temp_views(mock_spark)

        mock_spark.catalog.dropTempView.assert_not_called()


class TestSessionPythonSubmitter:
    """Tests for SessionPythonSubmitter."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        return MagicMock()

    @pytest.fixture
    def submitter(self, mock_spark):
        """Create a SessionPythonSubmitter with mock SparkSession."""
        return SessionPythonSubmitter(mock_spark)

    def test_submit_executes_code(self, submitter, mock_spark):
        """Test that submit executes the compiled code."""
        # Simple code that just sets a variable
        compiled_code = "result = 1 + 1"

        submitter.submit(compiled_code)

        # The code should execute without error

    def test_submit_provides_spark_in_globals(self, submitter, mock_spark):
        """Test that submit provides spark in the execution globals."""
        # Code that uses spark
        compiled_code = "spark_app_name = spark.sparkContext.appName"

        mock_spark.sparkContext.appName = "test-app"

        submitter.submit(compiled_code)

        # The code should execute without error

    def test_submit_raises_on_execution_error(self, submitter, mock_spark):
        """Test that submit raises DbtRuntimeError on execution error."""
        compiled_code = "raise ValueError('test error')"

        with pytest.raises(DbtRuntimeError) as exc_info:
            submitter.submit(compiled_code)

        assert "Python model execution failed" in str(exc_info.value)
        assert "test error" in str(exc_info.value)

    def test_submit_cleans_up_temp_views(self, submitter, mock_spark):
        """Test that submit cleans up temp views after execution."""
        compiled_code = "result = 1"
        mock_spark.sql.return_value.collect.return_value = []

        submitter.submit(compiled_code)

        # Cleanup should be called (SHOW VIEWS)
        mock_spark.sql.assert_called()


class TestSessionPythonJobHelper:
    """Tests for SessionPythonJobHelper."""

    @pytest.fixture
    def mock_credentials(self):
        """Create mock credentials."""
        creds = MagicMock()
        creds.is_session_mode = True
        return creds

    @pytest.fixture
    def parsed_model_dict(self):
        """Create a parsed model dictionary."""
        return {
            "catalog": "main",
            "schema": "test_schema",
            "identifier": "test_model",
            "config": {
                "timeout": 3600,
                "packages": [],
                "index_url": None,
                "additional_libs": [],
                "python_job_config": {},  # Empty dict instead of None
                "cluster_id": None,
                "http_path": None,
                "create_notebook": False,
                "job_cluster_config": {},  # Empty dict instead of None
                "access_control_list": [],
                "notebook_access_control_list": [],
                "user_folder_for_python": True,
                "environment_key": None,
                "environment_dependencies": [],
            },
        }

    def test_init_gets_spark_session(self, mock_credentials, parsed_model_dict):
        """Test that __init__ gets the SparkSession."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-123"
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        with patch.dict(
            "sys.modules",
            {"pyspark": MagicMock(), "pyspark.sql": MagicMock()},
        ):
            import sys

            sys.modules["pyspark.sql"].SparkSession.builder = mock_builder

            helper = SessionPythonJobHelper(parsed_model_dict, mock_credentials)

            mock_builder.getOrCreate.assert_called_once()
            assert helper._spark is mock_spark

    def test_submit_delegates_to_submitter(self, mock_credentials, parsed_model_dict):
        """Test that submit delegates to the submitter."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-123"
        mock_spark.sql.return_value.collect.return_value = []
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        with patch.dict(
            "sys.modules",
            {"pyspark": MagicMock(), "pyspark.sql": MagicMock()},
        ):
            import sys

            sys.modules["pyspark.sql"].SparkSession.builder = mock_builder

            helper = SessionPythonJobHelper(parsed_model_dict, mock_credentials)

            compiled_code = "result = 1"
            helper.submit(compiled_code)

            # The code should execute without error
