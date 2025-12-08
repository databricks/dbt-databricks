import pytest
from pydantic import ValidationError

from dbt.adapters.databricks.python_models.python_config import (
    ParsedPythonModel,
    PythonJobConfig,
    PythonModelConfig,
)


class TestParsedPythonModel:
    def test_parsed_model__default_database_schema(self):
        parsed_model = {
            "alias": "test",
            "config": {},
        }

        model = ParsedPythonModel(**parsed_model)
        assert model.catalog == "hive_metastore"
        assert model.schema_ == "default"
        assert model.identifier == "test"

    def test_parsed_model__empty_model_config(self):
        parsed_model = {
            "database": "database",
            "schema": "schema",
            "alias": "test",
            "config": {},
        }

        model = ParsedPythonModel(**parsed_model)
        assert model.catalog == "database"
        assert model.schema_ == "schema"
        assert model.identifier == "test"
        config = model.config
        assert config.user_folder_for_python is False
        assert config.timeout == 86400
        assert config.job_cluster_config == {}
        assert config.access_control_list == []
        assert config.packages == []
        assert config.index_url is None
        assert config.additional_libs == []
        assert config.python_job_config.name is None
        assert config.python_job_config.grants == {}
        assert config.python_job_config.existing_job_id == ""
        assert config.python_job_config.post_hook_tasks == []
        assert config.python_job_config.additional_task_settings == {}
        assert config.cluster_id is None
        assert config.http_path is None
        assert config.create_notebook is False

    def test_parsed_model__valid_model_config(self):
        parsed_model = {
            "alias": "test",
            "config": {
                "user_folder_for_python": True,
                "timeout": 100,
                "job_cluster_config": {"key": "value"},
                "access_control_list": [{"user_name": "user", "permission_level": "CAN_VIEW"}],
                "packages": ["package"],
                "index_url": "index_url",
                "additional_libs": [{"key": "value"}],
                "python_job_config": {"name": "name", "grants": {"view": [{"user_name": "test"}]}},
                "cluster_id": "cluster_id",
                "http_path": "http_path",
                "create_notebook": True,
            },
        }

        model = ParsedPythonModel(**parsed_model)
        config = model.config
        assert config.user_folder_for_python is True
        assert config.timeout == 100
        assert config.job_cluster_config == {"key": "value"}
        assert config.access_control_list == [{"user_name": "user", "permission_level": "CAN_VIEW"}]
        assert config.packages == ["package"]
        assert config.index_url == "index_url"
        assert config.additional_libs == [{"key": "value"}]
        assert config.python_job_config.name == "name"
        assert config.python_job_config.grants == {"view": [{"user_name": "test"}]}
        assert config.python_job_config.existing_job_id == ""
        assert config.python_job_config.post_hook_tasks == []
        assert config.python_job_config.additional_task_settings == {}
        assert config.cluster_id == "cluster_id"
        assert config.http_path == "http_path"
        assert config.create_notebook is True

    def test_parsed_model__extra_model_config(self):
        parsed_model = {
            "alias": "test",
            "config": {
                "python_job_config": {"foo": "bar"},
            },
        }

        model = ParsedPythonModel(**parsed_model)
        assert model.config.python_job_config.foo == "bar"

    def test_parsed_model__run_name(self):
        parsed_model = {"alias": "test", "config": {}}
        model = ParsedPythonModel(**parsed_model)
        assert model.run_name.startswith("hive_metastore-default-test-")

    def test_parsed_model__invalid_config(self):
        parsed_model = {"alias": "test", "config": 1}
        with pytest.raises(ValidationError):
            ParsedPythonModel(**parsed_model)


class TestPythonModelConfig:
    def test_python_model_config__invalid_timeout(self):
        config = {"timeout": -1}
        with pytest.raises(ValidationError):
            PythonModelConfig(**config)

    def test_python_model_config__defaults(self):
        config = PythonModelConfig()
        assert config.user_folder_for_python is False
        assert config.timeout == 86400
        assert config.job_cluster_config == {}
        assert config.access_control_list == []
        assert config.notebook_access_control_list == []
        assert config.packages == []
        assert config.index_url is None
        assert config.additional_libs == []
        assert config.cluster_id is None
        assert config.http_path is None
        assert config.create_notebook is False
        assert config.environment_key is None
        assert config.environment_dependencies == []

    def test_python_model_config__custom_values(self):
        config = PythonModelConfig(
            user_folder_for_python=True,
            timeout=3600,
            job_cluster_config={"spark_version": "12.2.x-scala2.12"},
            access_control_list=[{"user_name": "user", "permission_level": "CAN_VIEW"}],
            notebook_access_control_list=[{"user_name": "user", "permission_level": "CAN_READ"}],
            packages=["pandas", "numpy"],
            index_url="https://pypi.org/simple",
            additional_libs=[{"pypi": {"package": "requests"}}],
            python_job_config=PythonJobConfig(name="test_job"),
            cluster_id="1234",
            http_path="http://example.com",
            create_notebook=True,
            environment_key="test-env",
            environment_dependencies=["dep1", "dep2"],
        )

        assert config.user_folder_for_python is True
        assert config.timeout == 3600
        assert config.job_cluster_config == {"spark_version": "12.2.x-scala2.12"}
        assert config.access_control_list == [{"user_name": "user", "permission_level": "CAN_VIEW"}]
        assert config.notebook_access_control_list == [
            {"user_name": "user", "permission_level": "CAN_READ"}
        ]
        assert config.packages == ["pandas", "numpy"]
        assert config.index_url == "https://pypi.org/simple"
        assert config.additional_libs == [{"pypi": {"package": "requests"}}]
        assert config.python_job_config.name == "test_job"
        assert config.cluster_id == "1234"
        assert config.http_path == "http://example.com"
        assert config.create_notebook is True
        assert config.environment_key == "test-env"
        assert config.environment_dependencies == ["dep1", "dep2"]

    def test_python_model_config__invalid_job_permission(self):
        with pytest.raises(ValidationError) as exc_info:
            PythonModelConfig(
                access_control_list=[
                    {"user_name": "user", "permission_level": "INVALID_PERMISSION"}
                ]
            )
        assert "Invalid permission_level in access_control_list" in str(exc_info.value)

    def test_python_model_config__invalid_notebook_permission(self):
        with pytest.raises(ValidationError) as exc_info:
            PythonModelConfig(
                notebook_access_control_list=[
                    {"user_name": "user", "permission_level": "INVALID_PERMISSION"}
                ]
            )
        assert "Invalid permission_level in notebook_access_control_list" in str(exc_info.value)

    def test_python_model_config__missing_permission_level(self):
        with pytest.raises(ValidationError) as exc_info:
            PythonModelConfig(access_control_list=[{"user_name": "user"}])
        assert "permission_level is required in access_control_list" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            PythonModelConfig(notebook_access_control_list=[{"user_name": "user"}])
        assert "permission_level is required in notebook_access_control_list" in str(exc_info.value)


class TestPythonJobConfig:
    def test_python_job_config__dict_excludes_expected_fields(self):
        config = {
            "name": "name",
            "grants": {"view": [{"user": "user"}]},
            "existing_job_id": "existing_job_id",
            "post_hook_tasks": [{"task": "task"}],
            "additional_task_settings": {"key": "value"},
        }
        job_config = PythonJobConfig(**config).dict()
        assert job_config == {"name": "name"}

    def test_python_job_config__extra_values(self):
        config = {
            "name": "name",
            "existing_job_id": "existing_job_id",
            "foo": "bar",
        }
        job_config = PythonJobConfig(**config).dict()
        assert job_config == {"name": "name", "foo": "bar"}
