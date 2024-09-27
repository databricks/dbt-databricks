import uuid
from typing import Any
from typing import Dict
from typing import Optional

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.databricks.api_client import CommandExecution
from dbt.adapters.databricks.api_client import DatabricksApiClient
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.python_models.run_tracking import PythonRunTracker


DEFAULT_TIMEOUT = 60 * 60 * 24


class BaseDatabricksHelper(PythonJobHelper):
    tracker = PythonRunTracker()

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

        run_id = self.api_client.job_runs.submit(run_name, job_spec)
        self.tracker.insert_run_id(run_id)
        return run_id

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
        return f"{self.database}-{self.schema}-{self.identifier}__dbt"

    @property
    def notebook_path(self) -> str:
        return f"{self.notebook_dir}/{self.identifier}"

    @property
    def notebook_dir(self) -> str:
        return f"/Shared/dbt_python_model/{self.database}/{self.schema}"

    def check_credentials(self) -> None:
        workflow_config = self.parsed_model["config"].get("workflow_job_config", None)
        if not workflow_config:
            raise ValueError(
                "workflow_job_config is required for the `workflow_job_config` submission method."
            )

    def submit(self, compiled_code: str) -> None:
        workflow_spec = self.parsed_model["config"]["workflow_job_config"]
        cluster_spec = self.parsed_model["config"].get("job_cluster_config", None)

        # This dict gets modified throughout. Settings added through dbt are popped off
        # before the spec is sent to the Databricks API
        workflow_spec = self._build_job_spec(workflow_spec, cluster_spec)

        self._submit_through_workflow(compiled_code, workflow_spec)

    def _build_job_spec(self, workflow_spec, cluster_spec):
        workflow_spec["name"] = workflow_spec.get('name', self.default_job_name)

        cluster_settings = {}  # Undefined cluster settings defaults to serverless in the Databricks API
        if cluster_spec is not None:
            cluster_settings["new_cluster"] = cluster_spec
        elif 'existing_cluster_id' in workflow_spec:
            cluster_settings['existing_cluster_id'] = workflow_spec['existing_cluster_id']

        notebook_task = {
            'task_key': 'task_a',
            'notebook_task': {
                "notebook_path": self.notebook_path,
                "source": "WORKSPACE",
            },
        }
        notebook_task.update(cluster_settings)
        notebook_task.update(workflow_spec.pop("additional_task_settings", {}))

        post_hook_tasks = workflow_spec.pop("post_hook_tasks", [])
        for task in post_hook_tasks:
            if not 'existing_cluster_id' in task and not 'new_cluster' in task:
                task.update(cluster_settings)

        workflow_spec["tasks"] = [notebook_task] + post_hook_tasks
        return workflow_spec

    def _submit_through_workflow(self, compiled_code: str, workflow_spec) -> None:
        self.api_client.workspace.upload_notebook(self.notebook_path, compiled_code)

        job_id, is_new = self._get_or_create_job(workflow_spec)

        if not is_new:
            self._update_job(job_id, workflow_spec)

        grants = workflow_spec.pop("grants", {})
        self._update_job_permissions(job_id, grants)

        run_id = self._get_or_trigger_job_run(job_id)

        self.tracker.insert_run_id(run_id)
        self.polling(
            status_func=self.session.get,
            status_func_kwargs={
                "url": f"https://{self.credentials.host}/api/2.1/jobs/runs/get?run_id={run_id}",
                "headers": self.extra_headers,
            },
            get_state_func=lambda response: response.json()["state"]["life_cycle_state"],
            terminal_states=("TERMINATED", "SKIPPED", "INTERNAL_ERROR"),
            expected_end_state="TERMINATED",
            get_state_msg_func=lambda response: response.json()["state"]["state_message"],
        )

        run_output = self.api_client.session.get(
            f"https://{self.credentials.host}" f"/api/2.1/jobs/runs/get?run_id={run_id}"
        )
        json_run_output = run_output.json()

        result_state = json_run_output["state"]["result_state"]
        if result_state != "SUCCESS":
            raise DbtRuntimeError(
                "Python model failed with traceback as:\n"
                "(Note that the line number here does not "
                "match the line number in your code due to dbt templating)\n"
                f"{utils.remove_ansi(json_run_output['error_trace'])}"
            )
        self.tracker.remove_run_id(run_id)

    def _get_or_create_job(self, workflow_spec: dict) -> tuple[int, bool]:
        """
        :return: tuple of job_id and whether the job is new
        """
        existing_job_id = workflow_spec.pop('existing_job_id', '')
        if existing_job_id:
            return existing_job_id, False

        response = self.api_client.session.get(
            f"https://{self.credentials.host}/api/2.1/jobs/list",
            json={
                "name": workflow_spec['name'],
            }
        )

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error getting job.\n {response.content!r}")
        response_json = response.json()
        logger.info(f"Job list response={response_json}")

        response_jobs = response_json.get("jobs", [])
        if len(response_jobs) > 1:
            raise DbtRuntimeError(f"Multiple jobs found with name {workflow_spec['name']}")

        if len(response_jobs) == 1:
            return response_json["jobs"][0]["job_id"], False
        else:
            return self._create_job(workflow_spec), True

    def _create_job(self, workflow_spec: dict):
        """
        :return: the job id
        """
        response = self.api_client.session.post(
            f"https://{self.credentials.host}/api/2.1/jobs/create",
            headers=self.extra_headers,
            json=workflow_spec,
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating Databricks workflow.\n {response.content!r}")
        response_json = response.json()
        logger.info(f"Workflow create response={response_json}")
        return response_json["job_id"]

    def _update_job(self, job_id, workflow_spec):
        request_body = {
            "job_id": job_id,
            "new_settings": workflow_spec,
        }
        response = self.api_client.session.post(
            f"https://{self.credentials.host}/api/2.1/jobs/reset",
            headers=self.extra_headers,
            json=request_body,
        )

        logger.info(f"Workflow update response={response.json()}")
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error updating Databricks workflow.\n {response.content!r}")

    def _update_job_permissions(self, job_id, job_grants):
        access_control_list = self._build_job_permissions(job_id, job_grants)

        request_body = {
            "access_control_list": access_control_list
        }

        response = self.api_client.session.put(
            f"https://{self.credentials.host}/api/2.0/permissions/jobs/{job_id}",
            headers=self.extra_headers,
            json=request_body,
        )

        logger.info(f"Workflow permissions update response={response.json()}")
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error updating Databricks workflow.\n {response.content!r}")

    def _build_job_permissions(self, job_id, job_grants) -> list:
        access_control_list = []
        current_owner, permissions_attribute = self._get_current_job_owner(job_id)
        access_control_list.append({
            permissions_attribute: current_owner,
            'permission_level': 'IS_OWNER',
        })

        for grant in job_grants.get('view', []):
            acl_grant = grant.copy()
            acl_grant.update({
                'permission_level': 'CAN_VIEW',
            })
            access_control_list.append(acl_grant)
        for grant in job_grants.get('run', []):
            acl_grant = grant.copy()
            acl_grant.update({
                'permission_level': 'CAN_MANAGE_RUN',
            })
            access_control_list.append(acl_grant)
        for grant in job_grants.get('manage', []):
            acl_grant = grant.copy()
            acl_grant.update({
                'permission_level': 'CAN_MANAGE',
            })
            access_control_list.append(acl_grant)

        return access_control_list

    def _get_current_job_owner(self, job_id) -> tuple[str, str]:
        """
        :return: a tuple of the user id and the ACL attribute it came from ie:
            [user_name|group_name|service_principal_name]
            For example: `("mateizaharia@databricks.com", "user_name")`
        """
        response = self.api_client.session.get(
            f"https://{self.credentials.host}/api/2.0/permissions/jobs/{job_id}"
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error getting Databricks workflow permissions.\n {response.content!r}")

        for principal in response.json().get("access_control_list", []):
            for permission in principal['all_permissions']:
                if permission['permission_level'] == 'IS_OWNER' and permission['inherited'] is False:
                    if principal.get('user_name'):
                        return principal['user_name'], 'user_name'
                    elif principal.get('group_name'):
                        return principal['group_name'], 'group_name'
                    else:
                        return principal['service_principal_name'], 'service_principal_name'

        raise DbtRuntimeError(f"Error getting current owner for Databricks workflow.\n {response.content!r}")

    def _get_or_trigger_job_run(self, job_id: dict):
        """
        :return: the run id
        """
        active_runs_response = self.api_client.session.get(
            f"https://{self.credentials.host}/api/2.1/jobs/runs/list",
            json={
                'job_id': job_id,
                'active_only': True,
            }
        )
        if active_runs_response.status_code != 200:
            raise DbtRuntimeError(f"Error getting active runs.\n {active_runs_response.content!r}")

        active_runs = active_runs_response.json().get('runs', [])
        if len(active_runs) > 0:
            logger.info("Workflow already running, tracking active run instead of creating a new one")
            return active_runs[0]['run_id']

        request_body = {
            "job_id": job_id,
        }
        response = self.api_client.session.post(
            f"https://{self.credentials.host}/api/2.1/jobs/run-now",
            json=request_body
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error triggering a run for Databricks workflow.\n {response.content!r}")
        response_json = response.json()
        logger.info(f"Workflow trigger response={response_json}")

        return response_json["run_id"]
