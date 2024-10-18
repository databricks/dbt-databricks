from mock import patch
from unittest.mock import Mock

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.python_models.python_submissions import BaseDatabricksHelper
from dbt.adapters.databricks.python_models.python_submissions import WorkflowPythonJobHelper


# class TestDatabricksPythonSubmissions:
#     def test_start_cluster_returns_on_receiving_running_state(self):
#         session_mock = Mock()
#         # Mock the start command
#         post_mock = Mock()
#         post_mock.status_code = 200
#         session_mock.post.return_value = post_mock
#         # Mock the status command
#         get_mock = Mock()
#         get_mock.status_code = 200
#         get_mock.json.return_value = {"state": "RUNNING"}
#         session_mock.get.return_value = get_mock

#         context = DBContext(Mock(), None, None, session_mock)
#         context.start_cluster()

#         session_mock.get.assert_called_once()


class DatabricksTestHelper(BaseDatabricksHelper):
    def __init__(self, parsed_model: dict, credentials: DatabricksCredentials):
        self.parsed_model = parsed_model
        self.credentials = credentials
        self.job_grants = self.workflow_spec.get("grants", {})


class TestAclUpdate:
    def test_empty_acl_empty_config(self):
        helper = DatabricksTestHelper({"config": {}}, DatabricksCredentials())
        assert helper._update_with_acls({}) == {}

    def test_empty_acl_non_empty_config(self):
        helper = DatabricksTestHelper({"config": {}}, DatabricksCredentials())
        assert helper._update_with_acls({"a": "b"}) == {"a": "b"}

    def test_non_empty_acl_empty_config(self):
        expected_access_control = {
            "access_control_list": [
                {"user_name": "user2", "permission_level": "CAN_VIEW"},
            ]
        }
        helper = DatabricksTestHelper({"config": expected_access_control}, DatabricksCredentials())
        assert helper._update_with_acls({}) == expected_access_control

    def test_non_empty_acl_non_empty_config(self):
        expected_access_control = {
            "access_control_list": [
                {"user_name": "user2", "permission_level": "CAN_VIEW"},
            ]
        }
        helper = DatabricksTestHelper({"config": expected_access_control}, DatabricksCredentials())
        assert helper._update_with_acls({"a": "b"}) == {
            "a": "b",
            "access_control_list": expected_access_control["access_control_list"],
        }


class TestJobGrants:

    @patch.object(BaseDatabricksHelper, "_build_job_owner")
    def test_job_owner_user(self, mock_job_owner):
        mock_job_owner.return_value = ("alighodsi@databricks.com", "user_name")

        helper = DatabricksTestHelper({"config": {}}, DatabricksCredentials())
        helper.job_grants = {}

        assert helper._build_job_permissions() == [
            {
                "permission_level": "IS_OWNER",
                "user_name": "alighodsi@databricks.com",
            }
        ]

    @patch.object(BaseDatabricksHelper, "_build_job_owner")
    def test_job_owner_service_principal(self, mock_job_owner):
        mock_job_owner.return_value = (
            "9533b8cc-2d60-46dd-84f2-a39b3939e37a",
            "service_principal_name",
        )

        helper = DatabricksTestHelper({"config": {}}, DatabricksCredentials())
        helper.job_grants = {}

        assert helper._build_job_permissions() == [
            {
                "permission_level": "IS_OWNER",
                "service_principal_name": "9533b8cc-2d60-46dd-84f2-a39b3939e37a",
            }
        ]

    @patch.object(BaseDatabricksHelper, "_build_job_owner")
    def test_job_grants(self, mock_job_owner):
        mock_job_owner.return_value = (
            "9533b8cc-2d60-46dd-84f2-a39b3939e37a",
            "service_principal_name",
        )
        helper = DatabricksTestHelper(
            {
                "config": {
                    "python_job_config": {
                        "grants": {
                            "view": [
                                {"user_name": "reynoldxin@databricks.com"},
                                {"user_name": "alighodsi@databricks.com"},
                            ],
                            "run": [{"group_name": "dbt-developers"}],
                            "manage": [{"group_name": "dbt-admins"}],
                        }
                    }
                }
            },
            DatabricksCredentials(),
        )

        actual = helper._build_job_permissions()

        expected_owner = {
            "service_principal_name": "9533b8cc-2d60-46dd-84f2-a39b3939e37a",
            "permission_level": "IS_OWNER",
        }
        expected_viewer_1 = {
            "permission_level": "CAN_VIEW",
            "user_name": "reynoldxin@databricks.com",
        }
        expected_viewer_2 = {
            "permission_level": "CAN_VIEW",
            "user_name": "alighodsi@databricks.com",
        }
        expected_runner = {"permission_level": "CAN_MANAGE_RUN", "group_name": "dbt-developers"}
        expected_manager = {"permission_level": "CAN_MANAGE", "group_name": "dbt-admins"}

        assert expected_owner in actual
        assert expected_viewer_1 in actual
        assert expected_viewer_2 in actual
        assert expected_runner in actual
        assert expected_manager in actual


class TestWorkflowConfig:
    def default_config(self):
        return {
            "alias": "test_model",
            "database": "test_database",
            "schema": "test_schema",
            "config": {
                "python_job_config": {
                    "email_notifications": "test@example.com",
                    "max_retries": 2,
                    "timeout_seconds": 500,
                },
                "job_cluster_config": {
                    "spark_version": "15.3.x-scala2.12",
                    "node_type_id": "rd-fleet.2xlarge",
                    "autoscale": {"min_workers": 1, "max_workers": 2},
                },
            },
        }

    @patch("dbt.adapters.databricks.python_models.python_submissions.DatabricksApiClient")
    def test_build_job_spec_default(self, mock_api_client):
        job = WorkflowPythonJobHelper(self.default_config(), Mock())
        result = job._build_job_spec()

        assert result["name"] == "dbt__test_database-test_schema-test_model"
        assert len(result["tasks"]) == 1

        task = result["tasks"][0]
        assert task["task_key"] == "inner_notebook"
        assert task["new_cluster"]["spark_version"] == "15.3.x-scala2.12"

    @patch("dbt.adapters.databricks.python_models.python_submissions.DatabricksApiClient")
    def test_build_job_spec_custom_name(self, mock_api_client):
        config = self.default_config()
        config["config"]["python_job_config"]["name"] = "custom_job_name"
        job = WorkflowPythonJobHelper(config, Mock())
        result = job._build_job_spec()

        assert result["name"] == "custom_job_name"

    @patch("dbt.adapters.databricks.python_models.python_submissions.DatabricksApiClient")
    def test_build_job_spec_existing_cluster(self, mock_api_client):
        config = self.default_config()
        config["config"]["python_job_config"]["existing_cluster_id"] = "cluster-123"
        del config["config"]["job_cluster_config"]

        job = WorkflowPythonJobHelper(config, Mock())
        result = job._build_job_spec()

        task = result["tasks"][0]
        assert task["existing_cluster_id"] == "cluster-123"
        assert "new_cluster" not in task

    @patch("dbt.adapters.databricks.python_models.python_submissions.DatabricksApiClient")
    def test_build_job_spec_serverless(self, mock_api_client):
        config = self.default_config()
        del config["config"]["job_cluster_config"]

        job = WorkflowPythonJobHelper(config, Mock())
        result = job._build_job_spec()

        task = result["tasks"][0]
        assert "existing_cluster_id" not in task
        assert "new_cluster" not in task

    @patch("dbt.adapters.databricks.python_models.python_submissions.DatabricksApiClient")
    def test_build_job_spec_with_additional_task_settings(self, mock_api_client):
        config = self.default_config()
        config["config"]["python_job_config"]["additional_task_settings"] = {
            "task_key": "my_dbt_task"
        }
        job = WorkflowPythonJobHelper(config, Mock())
        result = job._build_job_spec()

        task = result["tasks"][0]
        assert task["task_key"] == "my_dbt_task"

    @patch("dbt.adapters.databricks.python_models.python_submissions.DatabricksApiClient")
    def test_build_job_spec_with_post_hooks(self, mock_api_client):
        config = self.default_config()
        config["config"]["python_job_config"]["post_hook_tasks"] = [
            {
                "depends_on": [{"task_key": "inner_notebook"}],
                "task_key": "task_b",
                "notebook_task": {
                    "notebook_path": "/Workspace/Shared/test_notebook",
                    "source": "WORKSPACE",
                },
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "rd-fleet.2xlarge",
                    "autoscale": {"min_workers": 1, "max_workers": 2},
                },
            }
        ]

        job = WorkflowPythonJobHelper(config, Mock())
        result = job._build_job_spec()

        assert len(result["tasks"]) == 2
        assert result["tasks"][1]["task_key"] == "task_b"
        assert result["tasks"][1]["new_cluster"]["spark_version"] == "14.3.x-scala2.12"
