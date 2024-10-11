import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.databricks.api_client import CommandExecution
from dbt.adapters.databricks.api_client import DatabricksApiClient
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.python_models.run_tracking import PythonRunTracker
from dbt_common.exceptions import DbtRuntimeError


DEFAULT_TIMEOUT = 60 * 60 * 24


class BaseDatabricksHelper(PythonJobHelper):
    tracker = PythonRunTracker()

    @property
    def workflow_spec(self) -> Dict[str, Any]:
        """
        The workflow gets modified throughout. Settings added through dbt are popped off
        before the spec is sent to the Databricks API
        """
        return self.parsed_model["config"].get("workflow_job_config", {})

    @property
    def cluster_spec(self) -> Dict[str, Any]:
        return self.parsed_model["config"].get("job_cluster_config", {})

    def __init__(self, parsed_model: Dict, credentials: DatabricksCredentials) -> None:
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.database = parsed_model.get("database")
        self.parsed_model = parsed_model
        use_user_folder = parsed_model["config"].get("user_folder_for_python", False)

        self.check_credentials()

        self.api_client = DatabricksApiClient.create(
            credentials, self.get_timeout(), use_user_folder
        )

        self.job_grants: Dict[str, List[Dict[str, Any]]] = self.workflow_spec.pop("grants", {})

    def get_timeout(self) -> int:
        timeout = self.parsed_model["config"].get("timeout", DEFAULT_TIMEOUT)
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        return timeout

    def check_credentials(self) -> None:
        self.credentials.validate_creds()

    def _update_with_acls(self, cluster_dict: dict) -> dict:
        acl = self.parsed_model["config"].get("access_control_list", None)
        if acl:
            cluster_dict.update({"access_control_list": acl})
        return cluster_dict

    def _build_job_permissions(self) -> List[Dict[str, Any]]:
        access_control_list = []
        owner, permissions_attribute = self._build_job_owner()
        access_control_list.append(
            {
                permissions_attribute: owner,
                "permission_level": "IS_OWNER",
            }
        )

        for grant in self.job_grants.get("view", []):
            acl_grant = grant.copy()
            acl_grant.update(
                {
                    "permission_level": "CAN_VIEW",
                }
            )
            access_control_list.append(acl_grant)
        for grant in self.job_grants.get("run", []):
            acl_grant = grant.copy()
            acl_grant.update(
                {
                    "permission_level": "CAN_MANAGE_RUN",
                }
            )
            access_control_list.append(acl_grant)
        for grant in self.job_grants.get("manage", []):
            acl_grant = grant.copy()
            acl_grant.update(
                {
                    "permission_level": "CAN_MANAGE",
                }
            )
            access_control_list.append(acl_grant)

        return access_control_list

    def _build_job_owner(self) -> Tuple[str, str]:
        """
        :return: a tuple of the user id and the ACL attribute it came from ie:
            [user_name|group_name|service_principal_name]
            For example: `("mateizaharia@databricks.com", "user_name")`
        """
        curr_user = self.api_client.curr_user.get_username()
        is_service_principal = self.api_client.curr_user.is_service_principal(curr_user)

        if is_service_principal:
            return curr_user, "service_principal_name"
        else:
            return curr_user, "user_name"

    def _submit_job(self, path: str, cluster_spec: dict) -> str:
        job_spec: Dict[str, Any] = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": path,
            },
        }
        job_spec.update(cluster_spec)  # updates 'new_cluster' config

        # PYPI packages
        packages = self.parsed_model["config"].get("packages", [])

        # custom index URL or default
        index_url = self.parsed_model["config"].get("index_url", None)

        # additional format of packages
        additional_libs = self.parsed_model["config"].get("additional_libs", [])
        libraries = []

        for package in packages:
            if index_url:
                libraries.append({"pypi": {"package": package, "repo": index_url}})
            else:
                libraries.append({"pypi": {"package": package}})

        for lib in additional_libs:
            libraries.append(lib)

        job_spec.update({"libraries": libraries})
        run_name = f"{self.database}-{self.schema}-{self.identifier}-{uuid.uuid4()}"

        additional_job_config = self._build_additional_job_settings()
        access_control_list = self._build_job_permissions()
        additional_job_config["access_control_list"] = access_control_list

        run_id = self.api_client.job_runs.submit(run_name, job_spec, **additional_job_config)
        self.tracker.insert_run_id(run_id)
        return run_id

    def _build_additional_job_settings(self) -> Dict[str, Any]:
        additional_configs = {}
        attrs_to_add = [
            "email_notifications",
            "webhook_notifications",
            "notification_settings",
            "timeout_seconds",
            "health",
            "environments",
        ]
        for attr in attrs_to_add:
            if attr in self.workflow_spec:
                additional_configs[attr] = self.workflow_spec[attr]

        return additional_configs

    def _submit_through_notebook(self, compiled_code: str, cluster_spec: dict) -> None:
        workdir = self.api_client.workspace.create_python_model_dir(
            self.database or "hive_metastore", self.schema
        )
        file_path = f"{workdir}{self.identifier}"

        self.api_client.workspace.upload_notebook(file_path, compiled_code)

        # submit job
        run_id = self._submit_job(file_path, cluster_spec)
        try:
            self.api_client.job_runs.poll_for_completion(run_id)
        finally:
            self.tracker.remove_run_id(run_id)

    def submit(self, compiled_code: str) -> None:
        raise NotImplementedError(
            "BasePythonJobHelper is an abstract class and you should implement submit method."
        )


class JobClusterPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self) -> None:
        super().check_credentials()
        if not self.parsed_model["config"].get("job_cluster_config", None):
            raise ValueError(
                "`job_cluster_config` is required for the `job_cluster` submission method."
            )

    def submit(self, compiled_code: str) -> None:
        cluster_spec = {"new_cluster": self.parsed_model["config"]["job_cluster_config"]}
        self._submit_through_notebook(compiled_code, self._update_with_acls(cluster_spec))


class AllPurposeClusterPythonJobHelper(BaseDatabricksHelper):
    @property
    def cluster_id(self) -> Optional[str]:
        return self.parsed_model["config"].get(
            "cluster_id",
            self.credentials.extract_cluster_id(
                self.parsed_model["config"].get("http_path", self.credentials.http_path)
            ),
        )

    def check_credentials(self) -> None:
        super().check_credentials()
        if not self.cluster_id:
            raise ValueError(
                "Databricks `http_path` or `cluster_id` of an all-purpose cluster is required "
                "for the `all_purpose_cluster` submission method."
            )

    def submit(self, compiled_code: str) -> None:
        assert (
            self.cluster_id is not None
        ), "cluster_id is required for all_purpose_cluster submission method."
        if self.parsed_model["config"].get("create_notebook", False):
            config = {}
            if self.cluster_id:
                config["existing_cluster_id"] = self.cluster_id
            self._submit_through_notebook(compiled_code, self._update_with_acls(config))
        else:
            context_id = self.api_client.command_contexts.create(self.cluster_id)
            command_exec: Optional[CommandExecution] = None
            try:
                command_exec = self.api_client.commands.execute(
                    self.cluster_id, context_id, compiled_code
                )

                self.tracker.insert_command(command_exec)
                # poll until job finish
                self.api_client.commands.poll_for_completion(command_exec)

            finally:
                if command_exec:
                    self.tracker.remove_command(command_exec)
                self.api_client.command_contexts.destroy(self.cluster_id, context_id)


class ServerlessClusterPythonJobHelper(BaseDatabricksHelper):
    def submit(self, compiled_code: str) -> None:
        self._submit_through_notebook(compiled_code, {})


class WorkflowPythonJobHelper(BaseDatabricksHelper):

    @property
    def default_job_name(self) -> str:
        return f"dbt__{self.database}-{self.schema}-{self.identifier}"

    @property
    def notebook_path(self) -> str:
        return f"{self.notebook_dir}/{self.identifier}"

    @property
    def notebook_dir(self) -> str:
        return self.api_client.workspace.user_api.get_folder(self.catalog, self.schema)

    @property
    def catalog(self) -> str:
        return self.database or "hive_metastore"

    def __init__(self, parsed_model: Dict, credentials: DatabricksCredentials) -> None:
        super().__init__(parsed_model, credentials)

        self.post_hook_tasks = self.workflow_spec.pop("post_hook_tasks", [])
        self.additional_task_settings = self.workflow_spec.pop("additional_task_settings", {})

    def check_credentials(self) -> None:
        workflow_config = self.parsed_model["config"].get("workflow_job_config", None)
        if not workflow_config:
            raise ValueError(
                "workflow_job_config is required for the `workflow_job_config` submission method."
            )

    def submit(self, compiled_code: str) -> None:
        workflow_spec = self._build_job_spec()
        self._submit_through_workflow(compiled_code, workflow_spec)

    def _build_job_spec(self) -> Dict[str, Any]:
        workflow_spec = dict(self.workflow_spec)
        workflow_spec["name"] = self.workflow_spec.get("name", self.default_job_name)

        # Undefined cluster settings defaults to serverless in the Databricks API
        cluster_settings = {}
        if self.cluster_spec:
            cluster_settings["new_cluster"] = self.cluster_spec
        elif "existing_cluster_id" in self.workflow_spec:
            cluster_settings["existing_cluster_id"] = self.workflow_spec["existing_cluster_id"]

        notebook_task = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": self.notebook_path,
                "source": "WORKSPACE",
            },
        }
        notebook_task.update(cluster_settings)
        notebook_task.update(self.additional_task_settings)

        workflow_spec["tasks"] = [notebook_task] + self.post_hook_tasks
        return workflow_spec

    def _submit_through_workflow(self, compiled_code: str, workflow_spec: Dict[str, Any]) -> None:
        self.api_client.workspace.create_python_model_dir(self.catalog, self.schema)
        self.api_client.workspace.upload_notebook(self.notebook_path, compiled_code)

        job_id, is_new = self._get_or_create_job(workflow_spec)

        if not is_new:
            self.api_client.workflows.update_job_settings(job_id, workflow_spec)

        access_control_list = self._build_job_permissions()
        self.api_client.workflow_permissions.put(job_id, access_control_list)

        run_id = self.api_client.workflows.run(job_id, enable_queueing=True)
        self.tracker.insert_run_id(run_id)

        try:
            self.api_client.job_runs.poll_for_completion(run_id)
        finally:
            self.tracker.remove_run_id(run_id)

    def _get_or_create_job(self, workflow_spec: Dict[str, Any]) -> Tuple[str, bool]:
        """
        :return: tuple of job_id and whether the job is new
        """
        existing_job_id = workflow_spec.pop("existing_job_id", "")
        if existing_job_id:
            return existing_job_id, False

        response_jobs = self.api_client.workflows.search_by_name(workflow_spec["name"])

        if len(response_jobs) > 1:
            raise DbtRuntimeError(
                f"""Multiple jobs found with name {workflow_spec['name']}. Use a unique job
                name or specify the `existing_job_id` in the workflow_job_config."""
            )

        if len(response_jobs) == 1:
            return response_jobs[0]["job_id"], False
        else:
            return self.api_client.workflows.create(workflow_spec), True
