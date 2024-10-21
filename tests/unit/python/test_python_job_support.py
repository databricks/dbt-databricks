from unittest.mock import Mock
import pytest

from dbt.adapters.databricks.python_models.python_submissions import (
    PythonJobConfigCompiler,
    PythonLibraryConfigurer,
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
    def identifier(self):
        return "identifier"

    @pytest.fixture
    def uploader(self, client, identifier):
        return PythonNotebookUploader(client, "", "", identifier)

    def test_upload__golden_path(self, uploader, client, compiled_code, workdir, identifier):
        client.workspace.create_python_model_dir.return_value = workdir

        file_path = uploader.upload(compiled_code)
        assert file_path == f"{workdir}{identifier}"
        client.workspace.upload_notebook.assert_called_once_with(file_path, compiled_code)

    def test_create__golden_path(self, client, parsed_model):
        parsed_model.catalog = "catalog"
        parsed_model.schema_ = "schema"
        parsed_model.identifier = "identifier"

        uploader = PythonNotebookUploader.create(client, parsed_model)
        assert uploader.catalog == "catalog"
        assert uploader.schema == "schema"
        assert uploader.identifier == "identifier"
        assert uploader.api_client == client


class TestPythonPermissionBuilder:
    def test_build_permission__no_grants_no_acls_user_owner(self, client):
        builder = PythonPermissionBuilder(client, {}, [])
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False
        acls = builder.build_job_permissions()
        assert acls == [{"user_name": "user", "permission_level": "IS_OWNER"}]

    def test_build_permission__no_grants_no_acls_sp_owner(self, client):
        builder = PythonPermissionBuilder(client, {}, [])
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = True
        acls = builder.build_job_permissions()
        assert acls == [{"service_principal_name": "user", "permission_level": "IS_OWNER"}]

    def test_build_permission__grants_no_acls(self, client):
        grants = {
            "view": [{"user_name": "user1"}],
            "run": [{"user_name": "user2"}],
            "manage": [{"user_name": "user3"}],
        }
        builder = PythonPermissionBuilder(client, grants, [])
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False

        expected = [
            {"user_name": "user", "permission_level": "IS_OWNER"},
            {"user_name": "user1", "permission_level": "CAN_VIEW"},
            {"user_name": "user2", "permission_level": "CAN_MANAGE_RUN"},
            {"user_name": "user3", "permission_level": "CAN_MANAGE"},
        ]

        assert builder.build_job_permissions() == expected

    def test_build_permission__grants_and_acls(self, client):
        grants = {
            "view": [{"user_name": "user1"}],
        }
        acls = [{"user_name": "user2", "permission_level": "CAN_MANAGE_RUN"}]
        builder = PythonPermissionBuilder(client, grants, acls)
        client.curr_user.get_username.return_value = "user"
        client.curr_user.is_service_principal.return_value = False

        expected = [
            {"user_name": "user", "permission_level": "IS_OWNER"},
            {"user_name": "user1", "permission_level": "CAN_VIEW"},
            {"user_name": "user2", "permission_level": "CAN_MANAGE_RUN"},
        ]

        assert builder.build_job_permissions() == expected

    def test_create__with_python_job_config(self, client, parsed_model):
        parsed_model.config.python_job_config.grants = {"view": [{"user_name": "user"}]}
        builder = PythonPermissionBuilder.create(client, parsed_model)

        assert builder.job_grants == {"view": [{"user_name": "user"}]}
        assert builder.acls == parsed_model.config.access_control_list

    def test_create__without_python_job_config(self, client, parsed_model):
        parsed_model.config.python_job_config = None
        builder = PythonPermissionBuilder.create(client, parsed_model)

        assert builder.job_grants == {}
        assert builder.acls == parsed_model.config.access_control_list


class TestPythonLibraryConfigurer:
    def test_get_library_config__no_packages_no_libraries(self):
        config = PythonLibraryConfigurer.get_library_config([], None, [])
        assert config == {"libraries": []}

    def test_get_library_config__packages_no_index_no_libraries(self):
        config = PythonLibraryConfigurer.get_library_config(["package1", "package2"], None, [])
        assert config == {
            "libraries": [{"pypi": {"package": "package1"}}, {"pypi": {"package": "package2"}}]
        }

    def test_get_library_config__packages_index_url_no_libraries(self):
        index_url = "http://example.com"
        config = PythonLibraryConfigurer.get_library_config(["package1", "package2"], index_url, [])
        assert config == {
            "libraries": [
                {"pypi": {"package": "package1", "repo": index_url}},
                {"pypi": {"package": "package2", "repo": index_url}},
            ]
        }

    def test_get_library_config__packages_libraries(self):
        config = PythonLibraryConfigurer.get_library_config(
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
    def cluster_spec(self):
        return {}

    @pytest.fixture
    def run_name(self):
        return "run_name"

    @pytest.fixture
    def additional_job_settings(self):
        return {}

    @pytest.fixture
    def compiler(self, client, permission_builder, run_name, cluster_spec, additional_job_settings):
        return PythonJobConfigCompiler(
            client, permission_builder, run_name, cluster_spec, additional_job_settings
        )

    def test_compile__empty_configs(self, compiler, run_name, permission_builder):
        permission_builder.build_job_permissions.return_value = []
        details = compiler.compile("path")
        assert details.run_name == run_name
        assert details.job_spec == {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": "path",
            },
        }
        assert details.additional_job_config == {}

    def test_compile__nonempty_configs(
        self, compiler, run_name, permission_builder, cluster_spec, additional_job_settings
    ):
        permission_builder.build_job_permissions.return_value = [
            {"user_name": "user", "permission_level": "IS_OWNER"}
        ]
        cluster_spec["libraries"] = [{"pypi": {"package": "package"}}]
        additional_job_settings["foo"] = "bar"
        details = compiler.compile("path")
        assert details.run_name == run_name
        assert details.job_spec == {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": "path",
            },
            "libraries": [{"pypi": {"package": "package"}}],
            "access_control_list": [{"user_name": "user", "permission_level": "IS_OWNER"}],
        }
        assert details.additional_job_config == {"foo": "bar"}

    def test_create__empty_configs(self, client, parsed_model, cluster_spec):
        parsed_model.config.packages = []
        parsed_model.config.additional_libs = []
        parsed_model.config.python_job_config = None
        compiler = PythonJobConfigCompiler.create(client, parsed_model, cluster_spec)
        assert compiler.api_client == client
        assert isinstance(compiler.permission_builder, PythonPermissionBuilder)
        assert compiler.run_name == parsed_model.run_name
        assert compiler.cluster_spec == cluster_spec
        assert compiler.additional_job_settings == {}

    def test_create__full_configs(self, client, parsed_model):
        cluster_spec = {"existing_cluster_id": "cluster_id"}
        parsed_model.config.packages = ["foo"]
        parsed_model.config.index_url = None
        parsed_model.config.additional_libs = [{"pypi": {"package": "bar"}}]
        parsed_model.config.python_job_config.dict.return_value = {"baz": "qux"}
        compiler = PythonJobConfigCompiler.create(client, parsed_model, cluster_spec)
        assert compiler.api_client == client
        assert isinstance(compiler.permission_builder, PythonPermissionBuilder)
        assert compiler.run_name == parsed_model.run_name
        assert compiler.cluster_spec == {
            "existing_cluster_id": "cluster_id",
            "libraries": [{"pypi": {"package": "foo"}}, {"pypi": {"package": "bar"}}],
        }
        assert compiler.additional_job_settings == {"baz": "qux"}
