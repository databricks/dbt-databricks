from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

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
        parsed_model.catalog = "catalog"
        parsed_model.schema_ = "schema"
        parsed_model.identifier = identifier
        parsed_model.config.notebook_scoped_libraries = False
        parsed_model.config.packages = []
        return PythonNotebookUploader(client, parsed_model)

    def test_upload__golden_path(self, uploader, client, compiled_code, workdir, identifier):
        client.workspace.create_python_model_dir.return_value = workdir
        uploader.set_notebook_permissions = Mock()
        uploader.job_grants = {}
        uploader.notebook_access_control_list = []

        file_path = uploader.upload(compiled_code)

        assert file_path == f"{workdir}{identifier}"
        client.workspace.upload_notebook.assert_called_once_with(file_path, compiled_code)
        uploader.set_notebook_permissions.assert_not_called()

    def test_upload__with_grants(self, uploader, client, compiled_code, workdir, identifier):
        client.workspace.create_python_model_dir.return_value = workdir

        uploader.job_grants = {"view": [{"group_name": "data-team"}]}
        uploader.notebook_access_control_list = []
        uploader.set_notebook_permissions = Mock()

        file_path = uploader.upload(compiled_code)

        assert file_path == f"{workdir}{identifier}"
        client.workspace.upload_notebook.assert_called_once_with(file_path, compiled_code)
        uploader.set_notebook_permissions.assert_called_once_with(file_path)

    def test_set_notebook_permissions__with_grants(self, uploader, client):
        permission_builder = Mock()
        python_submissions.PythonPermissionBuilder = Mock(return_value=permission_builder)
        permission_builder.build_permissions.return_value = [
            {"user_name": "owner", "permission_level": "IS_OWNER"},
            {"group_name": "data-team", "permission_level": "CAN_READ"},
        ]
        uploader.notebook_access_control_list = []

        uploader.set_notebook_permissions("/path/to/notebook")

        permission_builder.build_permissions.assert_called_once_with(
            uploader.job_grants, uploader.notebook_access_control_list, target_type="notebook"
        )
        client.notebook_permissions.put.assert_called_once_with(
            "/path/to/notebook",
            [
                {"user_name": "owner", "permission_level": "IS_OWNER"},
                {"group_name": "data-team", "permission_level": "CAN_READ"},
            ],
        )

    def test_set_notebook_permissions__no_acls(self, uploader, client):
        permission_builder = Mock()
        python_submissions.PythonPermissionBuilder = Mock(return_value=permission_builder)
        permission_builder.build_permissions.return_value = []
        uploader.notebook_access_control_list = []

        uploader.set_notebook_permissions("/path/to/notebook")

        permission_builder.build_permissions.assert_called_once_with(
            uploader.job_grants, uploader.notebook_access_control_list, target_type="notebook"
        )
        client.notebook_permissions.put.assert_not_called()

    def test_set_notebook_permissions__exception_handled(self, uploader, client):
        permission_builder = Mock()
        python_submissions.PythonPermissionBuilder = Mock(return_value=permission_builder)
        permission_builder.build_permissions.return_value = [
            {"user_name": "owner", "permission_level": "IS_OWNER"}
        ]
        client.notebook_permissions.put.side_effect = Exception("API error")

        with pytest.raises(DbtRuntimeError) as exc_info:
            uploader.set_notebook_permissions("/path/to/notebook")

        assert "Failed to set permissions on notebook" in str(exc_info.value)
        permission_builder.build_permissions.assert_called_once()
        client.notebook_permissions.put.assert_called_once()


class TestPythonPermissionBuilder:
    @pytest.fixture
    def builder(self, client):
        return PythonPermissionBuilder(client)

    def test_build_job_permission__no_grants_no_acls_user_owner(self, builder, client):
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False
        acls = builder.build_job_permissions({}, [])
        assert acls == [{"user_name": "user", "permission_level": "IS_OWNER"}]

    def test_build_job_permission__no_grants_no_acls_sp_owner(self, builder, client):
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = True
        acls = builder.build_job_permissions({}, [])
        assert acls == [{"service_principal_name": "user", "permission_level": "IS_OWNER"}]

    def test_build_job_permission__grants_no_acls(self, builder, client):
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

    def test_build_notebook_permission__grants_no_acls(self, builder, client):
        grants = {
            "view": [{"user_name": "user1"}],
            "run": [{"user_name": "user2"}],
            "manage": [{"user_name": "user3"}],
        }
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False

        expected = [
            {"user_name": "user1", "permission_level": "CAN_READ"},
            {"user_name": "user2", "permission_level": "CAN_RUN"},
            {"user_name": "user3", "permission_level": "CAN_MANAGE"},
        ]

        assert builder.build_notebook_permissions(grants, []) == expected

    def test_build_job_permission__grants_and_acls(self, builder, client):
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

    def test_get_library_config__notebook_scoped_packages_excluded(self):
        config = python_submissions.get_library_config(
            ["package1", "package2"], None, [], notebook_scoped_libraries=True
        )
        assert config == {"libraries": []}

    def test_get_library_config__notebook_scoped_with_additional_libs(self):
        config = python_submissions.get_library_config(
            ["package1", "package2"],
            None,
            [{"jar": "s3://mybucket/myjar.jar"}],
            notebook_scoped_libraries=True,
        )
        assert config == {"libraries": [{"jar": "s3://mybucket/myjar.jar"}]}

    def test_get_library_config__notebook_scoped_false_includes_packages(self):
        config = python_submissions.get_library_config(
            ["package1", "package2"], None, [], notebook_scoped_libraries=False
        )
        assert config == {
            "libraries": [{"pypi": {"package": "package1"}}, {"pypi": {"package": "package2"}}]
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
        parsed_model.config.notebook_scoped_libraries = False
        return run_name

    @pytest.fixture
    def environment_key(self, parsed_model):
        environment_key = "test_key"
        parsed_model.config.environment_key = environment_key
        parsed_model.config.environment_dependencies = ["requests"]
        return environment_key

    def test_compile__empty_configs(self, client, permission_builder, parsed_model, run_name):
        parsed_model.config.python_job_config.dict.return_value = {}
        parsed_model.config.environment_key = None
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

    def test_compile__nonempty_configs(
        self, client, permission_builder, parsed_model, run_name, environment_key
    ):
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
            "environment_key": environment_key,
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": "path",
            },
            "cluster_id": "id",
            "libraries": [{"pypi": {"package": "foo"}}],
            "access_control_list": [{"user_name": "user", "permission_level": "IS_OWNER"}],
            "queue": {"enabled": True},
        }
        assert details.additional_job_config == {
            "foo": "bar",
            "environments": [
                {
                    "environment_key": environment_key,
                    "spec": {"environment_version": "4", "dependencies": ["requests"]},
                }
            ],
        }

    def test_compile__uses_environment_version_not_deprecated_client(
        self, client, permission_builder, parsed_model, run_name, environment_key
    ):
        """Test that environment_version is used instead of deprecated 'client' field.

        See GitHub issue #1277: The Databricks API deprecated the 'client' field
        in favor of 'environment_version'.
        """
        parsed_model.config.packages = []
        parsed_model.config.index_url = None
        parsed_model.config.python_job_config.dict.return_value = {}

        permission_builder.build_job_permissions.return_value = []
        compiler = PythonJobConfigCompiler(client, permission_builder, parsed_model, {})
        details = compiler.compile("path")

        # Verify environment_version is used, not client
        env_spec = details.additional_job_config["environments"][0]["spec"]
        assert "environment_version" in env_spec, (
            "Should use 'environment_version' not deprecated 'client'"
        )
        assert "client" not in env_spec, "Should not use deprecated 'client' field"

    def test_compile__respects_user_provided_environments(
        self, client, permission_builder, parsed_model, run_name
    ):
        """Test that user-provided environments in python_job_config are respected.

        See GitHub issue #1277: Users should be able to specify their own
        environments configuration to control serverless version.
        """
        parsed_model.config.packages = []
        parsed_model.config.index_url = None
        parsed_model.config.environment_key = "custom_env"
        parsed_model.config.environment_dependencies = []  # No auto-generated deps

        # User provides their own environments configuration
        user_environments = [
            {
                "environment_key": "custom_env",
                "spec": {"environment_version": "3"},
            }
        ]
        parsed_model.config.python_job_config.dict.return_value = {
            "environments": user_environments
        }

        permission_builder.build_job_permissions.return_value = []
        compiler = PythonJobConfigCompiler(client, permission_builder, parsed_model, {})
        details = compiler.compile("path")

        # User-provided environments should be preserved
        assert details.additional_job_config["environments"] == user_environments
        # The environment_key should still be set in job_spec
        assert details.job_spec["environment_key"] == "custom_env"

    def test_compile__user_environments_override_auto_generated(
        self, client, permission_builder, parsed_model, run_name
    ):
        """Test that user-provided environments override auto-generated ones.

        See GitHub issue #1277: Even when environment_dependencies are set,
        user-provided environments should take precedence.
        """
        parsed_model.config.packages = []
        parsed_model.config.index_url = None
        parsed_model.config.environment_key = "my_env"
        parsed_model.config.environment_dependencies = ["pandas", "numpy"]  # Would trigger auto-gen

        # User provides their own environments with specific version
        user_environments = [
            {
                "environment_key": "my_env",
                "spec": {"environment_version": "3", "dependencies": ["requests"]},
            }
        ]
        parsed_model.config.python_job_config.dict.return_value = {
            "environments": user_environments
        }

        permission_builder.build_job_permissions.return_value = []
        compiler = PythonJobConfigCompiler(client, permission_builder, parsed_model, {})
        details = compiler.compile("path")

        # User-provided environments should override, not be merged
        assert details.additional_job_config["environments"] == user_environments
        # Should have user's dependencies, not auto-generated ones
        assert details.additional_job_config["environments"][0]["spec"]["dependencies"] == [
            "requests"
        ]

    def test_compile__notebook_scoped_libraries_excludes_packages(
        self, client, permission_builder, parsed_model, run_name
    ):
        parsed_model.config.packages = ["pandas", "numpy"]
        parsed_model.config.index_url = None
        parsed_model.config.notebook_scoped_libraries = True
        parsed_model.config.environment_key = None
        parsed_model.config.python_job_config.dict.return_value = {}

        permission_builder.build_job_permissions.return_value = []
        compiler = PythonJobConfigCompiler(client, permission_builder, parsed_model, {})
        details = compiler.compile("path")

        # Libraries should be empty since packages are notebook-scoped
        assert details.job_spec["libraries"] == []

    def test_compile__notebook_scoped_false_includes_packages(
        self, client, permission_builder, parsed_model, run_name
    ):
        parsed_model.config.packages = ["pandas", "numpy"]
        parsed_model.config.index_url = None
        parsed_model.config.notebook_scoped_libraries = False
        parsed_model.config.environment_key = None
        parsed_model.config.python_job_config.dict.return_value = {}

        permission_builder.build_job_permissions.return_value = []
        compiler = PythonJobConfigCompiler(client, permission_builder, parsed_model, {})
        details = compiler.compile("path")

        # Libraries should include packages
        assert details.job_spec["libraries"] == [
            {"pypi": {"package": "pandas"}},
            {"pypi": {"package": "numpy"}},
        ]
