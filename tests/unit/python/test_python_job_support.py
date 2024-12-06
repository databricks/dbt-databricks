from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.python_models import python_submissions
from dbt.adapters.databricks.python_models.python_submissions import (
    PythonJobConfigCompiler,
    PythonNotebookUploader,
    PythonPermissionBuilder,
)


@pytest.fixture
def client():
    return Mock()


@pytest.fixture
def compiled_code():
    return "compiled_code"


@pytest.fixture
def parsed_model():
    return Mock()


class TestPythonNotebookUploader:
    @pytest.fixture
    def workdir(self):
        return "workdir"

    @pytest.fixture
    def identifier(self, parsed_model):
        return "identifier"

    @pytest.fixture
    def uploader(self, client, parsed_model, identifier):
        parsed_model.identifier = identifier
        return PythonNotebookUploader(client, parsed_model)

    def test_upload__golden_path(self, uploader, client, compiled_code, workdir, identifier):
        client.workspace.create_python_model_dir.return_value = workdir

        file_path = uploader.upload(compiled_code)
        assert file_path == f"{workdir}/{identifier}"
        client.workspace.upload_notebook.assert_called_once_with(file_path, compiled_code)


class TestPythonPermissionBuilder:
    @pytest.fixture
    def builder(self, client):
        return PythonPermissionBuilder(client)

    def test_build_permission__no_grants_no_acls_user_owner(self, builder, client):
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False
        acls = builder.build_job_permissions({}, [])
        assert acls == [{"user_name": "user", "permission_level": "IS_OWNER"}]

    def test_build_permission__no_grants_no_acls_sp_owner(self, builder, client):
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = True
        acls = builder.build_job_permissions({}, [])
        assert acls == [{"service_principal_name": "user", "permission_level": "IS_OWNER"}]

    def test_build_permission__grants_no_acls(self, builder, client):
        grants = {
            "view": [{"user_name": "user1"}],
            "run": [{"user_name": "user2"}],
            "manage": [{"user_name": "user3"}],
        }
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False

        expected = [
            {"user_name": "user", "permission_level": "IS_OWNER"},
            {"user_name": "user1", "permission_level": "CAN_VIEW"},
            {"user_name": "user2", "permission_level": "CAN_MANAGE_RUN"},
            {"user_name": "user3", "permission_level": "CAN_MANAGE"},
        ]

        assert builder.build_job_permissions(grants, []) == expected

    def test_build_permission__grants_and_acls(self, builder, client):
        grants = {
            "view": [{"user_name": "user1"}],
        }
        acls = [{"user_name": "user2", "permission_level": "CAN_MANAGE_RUN"}]
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False

        expected = [
            {"user_name": "user", "permission_level": "IS_OWNER"},
            {"user_name": "user1", "permission_level": "CAN_VIEW"},
            {"user_name": "user2", "permission_level": "CAN_MANAGE_RUN"},
        ]

        assert builder.build_job_permissions(grants, acls) == expected


class TestGetLibraryConfig:
    def test_get_library_config__no_packages_no_libraries(self):
        config = python_submissions.get_library_config([], None, [])
        assert config == {"libraries": []}

    def test_get_library_config__packages_no_index_no_libraries(self):
        config = python_submissions.get_library_config(["package1", "package2"], None, [])
        assert config == {
            "libraries": [{"pypi": {"package": "package1"}}, {"pypi": {"package": "package2"}}]
        }

    def test_get_library_config__packages_index_url_no_libraries(self):
        index_url = "http://example.com"
        config = python_submissions.get_library_config(["package1", "package2"], index_url, [])
        assert config == {
            "libraries": [
                {"pypi": {"package": "package1", "repo": index_url}},
                {"pypi": {"package": "package2", "repo": index_url}},
            ]
        }

    def test_get_library_config__packages_libraries(self):
        config = python_submissions.get_library_config(
            ["package1", "package2"], None, [{"pypi": {"package": "package3"}}]
        )
        assert config == {
            "libraries": [
                {"pypi": {"package": "package1"}},
                {"pypi": {"package": "package2"}},
                {"pypi": {"package": "package3"}},
            ]
        }


class TestPythonJobConfigCompiler:
    @pytest.fixture
    def permission_builder(self):
        return Mock()

    @pytest.fixture
    def run_name(self, parsed_model):
        run_name = "run_name"
        parsed_model.run_name = run_name
        parsed_model.config.packages = []
        parsed_model.config.additional_libs = []
        return run_name

    def test_compile__empty_configs(self, client, permission_builder, parsed_model, run_name):
        parsed_model.config.python_job_config.dict.return_value = {}
        compiler = PythonJobConfigCompiler(client, permission_builder, parsed_model, {})
        permission_builder.build_job_permissions.return_value = []
        details = compiler.compile("path")
        assert details.run_name == run_name
        assert details.job_spec == {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": "path",
            },
            "libraries": [],
            "queue": {"enabled": True},
        }
        assert details.additional_job_config == {}

    def test_compile__nonempty_configs(self, client, permission_builder, parsed_model, run_name):
        parsed_model.config.packages = ["foo"]
        parsed_model.config.index_url = None
        parsed_model.config.python_job_config.dict.return_value = {"foo": "bar"}

        permission_builder.build_job_permissions.return_value = [
            {"user_name": "user", "permission_level": "IS_OWNER"}
        ]
        compiler = PythonJobConfigCompiler(
            client, permission_builder, parsed_model, {"cluster_id": "id"}
        )
        details = compiler.compile("path")
        assert details.run_name == run_name
        assert details.job_spec == {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": "path",
            },
            "cluster_id": "id",
            "libraries": [{"pypi": {"package": "foo"}}],
            "access_control_list": [{"user_name": "user", "permission_level": "IS_OWNER"}],
            "queue": {"enabled": True},
        }
        assert details.additional_job_config == {"foo": "bar"}
