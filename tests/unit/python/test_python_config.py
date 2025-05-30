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
