from mock import patch

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.python_models.python_submissions import BaseDatabricksHelper


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
        self.job_grants = self.workflow_spec.pop("grants", {})


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
                    "workflow_job_config": {
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
    pass
