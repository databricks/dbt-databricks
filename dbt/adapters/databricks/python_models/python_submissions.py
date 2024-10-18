from abc import ABC, abstractmethod
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from attr import dataclass
from typing_extensions import override

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.databricks.api_client import CommandExecution, WorkflowJobApi
from dbt.adapters.databricks.api_client import DatabricksApiClient
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.python_models.python_config import ParsedPythonModel
from dbt.adapters.databricks.python_models.run_tracking import PythonRunTracker
from dbt_common.exceptions import DbtRuntimeError


DEFAULT_TIMEOUT = 60 * 60 * 24


class PythonSubmitter(ABC):
    @abstractmethod
    def submit(self, compiled_code: str) -> None:
        pass


class BaseDatabricksHelper(PythonJobHelper):
    tracker = PythonRunTracker()

    def __init__(self, parsed_model: Dict, credentials: DatabricksCredentials) -> None:
        self.credentials = credentials
        self.credentials.validate_creds()
        self.parsed_model = ParsedPythonModel(**parsed_model)

        self.api_client = DatabricksApiClient.create(
            credentials,
            self.parsed_model.config.timeout,
            self.parsed_model.config.user_folder_for_python,
        )
        self.validate_config()

        self.command_submitter = self.build_submitter()

    @abstractmethod
    def build_submitter(self) -> PythonSubmitter:
        pass

    def validate_config(self) -> None:
        pass

    def submit(self, compiled_code: str) -> None:
        self.command_submitter.submit(compiled_code)


class PythonCommandSubmitter(PythonSubmitter):
    def __init__(
        self, api_client: DatabricksApiClient, tracker: PythonRunTracker, cluster_id: str
    ) -> None:
        self.api_client = api_client
        self.tracker = tracker
        self.cluster_id = cluster_id

    @override
    def submit(self, compiled_code: str) -> None:
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


class PythonNotebookUploader:
    def __init__(
        self, api_client: DatabricksApiClient, database: str, schema: str, identifier: str
    ) -> None:
        self.api_client = api_client
        self.database = database
        self.schema = schema
        self.identifier = identifier

    def upload(self, compiled_code: str) -> str:
        workdir = self.api_client.workspace.create_python_model_dir(self.database, self.schema)
        file_path = f"{workdir}{self.identifier}"
        self.api_client.workspace.upload_notebook(file_path, compiled_code)
        return file_path


@dataclass(frozen=True)
class PythonJobDetails:
    run_name: str
    job_spec: Dict[str, Any]
    additional_job_config: Dict[str, Any]


class PythonPermissionBuilder:
    def get_job_owner_for_config(self, api_client: DatabricksApiClient) -> Tuple[str, str]:
        curr_user = api_client.curr_user.get_username()
        is_service_principal = api_client.curr_user.is_service_principal(curr_user)

        source = "service_principal_name" if is_service_principal else "user_name"
        return curr_user, source

    def build_job_permissions(
        self, api_client: DatabricksApiClient, job_grants: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        access_control_list = []
        owner, permissions_attribute = self.get_job_owner_for_config(api_client)
        access_control_list.append(
            {
                permissions_attribute: owner,
                "permission_level": "IS_OWNER",
            }
        )

        for grant in job_grants.get("view", []):
            acl_grant = grant.copy()
            acl_grant.update(
                {
                    "permission_level": "CAN_VIEW",
                }
            )
            access_control_list.append(acl_grant)
        for grant in job_grants.get("run", []):
            acl_grant = grant.copy()
            acl_grant.update(
                {
                    "permission_level": "CAN_MANAGE_RUN",
                }
            )
            access_control_list.append(acl_grant)
        for grant in job_grants.get("manage", []):
            acl_grant = grant.copy()
            acl_grant.update(
                {
                    "permission_level": "CAN_MANAGE",
                }
            )
            access_control_list.append(acl_grant)

        return access_control_list


class PythonJobConfigCompiler:
    def __init__(
        self,
        api_client: DatabricksApiClient,
        permission_builder: PythonPermissionBuilder,
        model: ParsedPythonModel,
        cluster_spec: Dict[str, Any],
    ) -> None:
        self.api_client = api_client
        self.permission_builder = permission_builder
        self.run_name = model.run_name
        self.acls = model.config.access_control_list
        self.packages = model.config.packages
        self.index_url = model.config.index_url
        self.additional_libraries = model.config.additional_libs
        if model.config.python_job_config:
            self.job_grants = model.config.python_job_config.grants
            self.additional_job_settings = model.config.python_job_config.dict()
        else:
            self.job_grants = {}
            self.additional_job_settings = {}
        self.cluster_spec = cluster_spec

    def update_with_acls(self, cluster_dict: Dict[str, Any]) -> Dict[str, Any]:
        local = cluster_dict.copy()

        if self.acls:
            local.update({"access_control_list": self.acls})
        return local

    def update_with_libraries(self, job_spec: Dict[str, Any]) -> Dict[str, Any]:
        local = job_spec.copy()
        libraries = []

        for package in self.packages:
            if self.index_url:
                libraries.append({"pypi": {"package": package, "repo": self.index_url}})
            else:
                libraries.append({"pypi": {"package": package}})

        for library in self.additional_libraries:
            libraries.append(library)

        local.update({"libraries": libraries})
        return local

    def compile(self, path: str) -> PythonJobDetails:
        job_spec: Dict[str, Any] = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": path,
            },
        }
        cluster_spec = self.update_with_acls(self.cluster_spec)
        job_spec.update(cluster_spec)  # updates 'new_cluster' config

        job_spec = self.update_with_libraries(job_spec)

        additional_job_config = self.additional_job_settings
        access_control_list = self.permission_builder.build_job_permissions(
            self.api_client, self.job_grants
        )
        additional_job_config["access_control_list"] = access_control_list

        return PythonJobDetails(self.run_name, job_spec, additional_job_config)


class PythonNotebookSubmitter(PythonSubmitter):
    def __init__(
        self,
        api_client: DatabricksApiClient,
        tracker: PythonRunTracker,
        uploader: PythonNotebookUploader,
        config_compiler: PythonJobConfigCompiler,
    ) -> None:
        self.api_client = api_client
        self.tracker = tracker
        self.uploader = uploader
        self.config_compiler = config_compiler

    @override
    def submit(self, compiled_code: str) -> None:
        file_path = self.uploader.upload(compiled_code)
        job_config = self.config_compiler.compile(file_path)

        # submit job
        run_id = self.api_client.job_runs.submit(
            job_config.run_name, job_config.job_spec, **job_config.additional_job_config
        )
        self.tracker.insert_run_id(run_id)
        try:
            self.api_client.job_runs.poll_for_completion(run_id)
        finally:
            self.tracker.remove_run_id(run_id)


class JobClusterPythonJobHelper(BaseDatabricksHelper):
    @override
    def build_submitter(self) -> PythonSubmitter:
        notebook_uploader = PythonNotebookUploader(
            self.api_client,
            self.parsed_model.catalog,
            self.parsed_model.schema_,
            self.parsed_model.identifier,
        )
        config_compiler = PythonJobConfigCompiler(
            self.api_client,
            PythonPermissionBuilder(),
            self.parsed_model,
            {"new_cluster": self.parsed_model.config.job_cluster_config},
        )
        return PythonNotebookSubmitter(
            self.api_client, self.tracker, notebook_uploader, config_compiler
        )

    @override
    def validate_config(self) -> None:
        if not self.parsed_model.config.job_cluster_config:
            raise ValueError(
                "`job_cluster_config` is required for the `job_cluster` submission method."
            )


class AllPurposeClusterPythonJobHelper(BaseDatabricksHelper):
    def build_submitter(self) -> PythonSubmitter:
        config = self.parsed_model.config
        if config.create_notebook:
            notebook_uploader = PythonNotebookUploader(
                self.api_client,
                self.parsed_model.catalog,
                self.parsed_model.schema_,
                self.parsed_model.identifier,
            )
            config_compiler = PythonJobConfigCompiler(
                self.api_client,
                PythonPermissionBuilder(),
                self.parsed_model,
                {"existing_cluster_id": self.cluster_id},
            )
            return PythonNotebookSubmitter(
                self.api_client, self.tracker, notebook_uploader, config_compiler
            )
        else:
            return PythonCommandSubmitter(self.api_client, self.tracker, self.cluster_id or "")

    @override
    def validate_config(self) -> None:
        config = self.parsed_model.config
        self.cluster_id = config.cluster_id or self.credentials.extract_cluster_id(
            config.http_path or self.credentials.http_path or ""
        )
        if not self.cluster_id:
            raise ValueError(
                "Databricks `http_path` or `cluster_id` of an all-purpose cluster is required "
                "for the `all_purpose_cluster` submission method."
            )


class ServerlessClusterPythonJobHelper(BaseDatabricksHelper):
    def build_submitter(self) -> PythonSubmitter:
        notebook_uploader = PythonNotebookUploader(
            self.api_client,
            self.parsed_model.catalog,
            self.parsed_model.schema_,
            self.parsed_model.identifier,
        )
        config_compiler = PythonJobConfigCompiler(
            self.api_client,
            PythonPermissionBuilder(),
            self.parsed_model,
            {},
        )
        return PythonNotebookSubmitter(
            self.api_client, self.tracker, notebook_uploader, config_compiler
        )


class PythonWorkflowConfigCompiler:
    def __init__(self, parsed_model: ParsedPythonModel):
        config = parsed_model.config
        self.job_cluster_config = config.job_cluster_config
        if config.python_job_config:
            self.existing_cluster_id = config.python_job_config.existing_job_id or ""
            self.additional_task_settings = config.python_job_config.additional_task_settings
            self.workflow_spec = config.python_job_config.dict()
            workflow_name = (
                config.python_job_config.name
                or f"dbt__{parsed_model.catalog}-{parsed_model.schema_}-{parsed_model.identifier}"
            )
            self.workflow_spec["name"] = workflow_name
            self.post_hook_tasks = config.python_job_config.post_hook_tasks
        else:
            self.existing_cluster_id = ""
            self.additional_task_settings = {}
            self.workflow_spec = {}
            self.post_hook_tasks = []

    def compile(self, path: str) -> Tuple[Dict[str, Any], str]:
        # Undefined cluster settings defaults to serverless in the Databricks API
        cluster_settings: Dict[str, Any] = {}
        if self.job_cluster_config:
            cluster_settings["new_cluster"] = self.job_cluster_config
        elif self.existing_cluster_id:
            cluster_settings["existing_cluster_id"] = self.existing_cluster_id

        notebook_task = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": path,
                "source": "WORKSPACE",
            },
        }
        notebook_task.update(cluster_settings)
        notebook_task.update(self.additional_task_settings)

        self.workflow_spec["tasks"] = [notebook_task] + self.post_hook_tasks
        return self.workflow_spec, self.existing_cluster_id


class PythonWorkflowCreater:
    def __init__(self, workflows: WorkflowJobApi) -> None:
        self.workflows = workflows

    def create_or_update(
        self,
        workflow_spec: Dict[str, Any],
        existing_job_id: Optional[str],
    ) -> str:
        """
        :return: tuple of job_id and whether the job is new
        """
        if not existing_job_id:
            workflow_name = workflow_spec["name"]
            response_jobs = self.workflows.search_by_name(workflow_name)

            if len(response_jobs) > 1:
                raise DbtRuntimeError(
                    f"Multiple jobs found with name {workflow_name}. Use a"
                    " unique job name or specify the `existing_job_id` in the python_job_config."
                )

            if len(response_jobs) == 1:
                existing_job_id = response_jobs[0]["job_id"]
            else:
                return self.workflows.create(workflow_spec)

        assert existing_job_id is not None
        self.workflows.update_job_settings(existing_job_id, workflow_spec)
        return existing_job_id


class PythonNotebookWorkflowSubmitter(PythonSubmitter):
    def __init__(
        self,
        api_client: DatabricksApiClient,
        tracker: PythonRunTracker,
        uploader: PythonNotebookUploader,
        config_compiler: PythonWorkflowConfigCompiler,
        permission_builder: PythonPermissionBuilder,
        workflow_creater: PythonWorkflowCreater,
        job_grants: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        self.api_client = api_client
        self.tracker = tracker
        self.uploader = uploader
        self.config_compiler = config_compiler
        self.permission_builder = permission_builder
        self.workflow_creater = workflow_creater
        self.job_grants = job_grants

    @override
    def submit(self, compiled_code: str) -> None:
        file_path = self.uploader.upload(compiled_code)

        workflow_config, existing_job_id = self.config_compiler.compile(file_path)
        job_id = self.workflow_creater.create_or_update(workflow_config, existing_job_id)

        access_control_list = self.permission_builder.build_job_permissions(
            self.api_client, self.job_grants
        )
        self.api_client.workflow_permissions.put(job_id, access_control_list)

        run_id = self.api_client.workflows.run(job_id, enable_queueing=True)
        self.tracker.insert_run_id(run_id)

        try:
            self.api_client.job_runs.poll_for_completion(run_id)
        finally:
            self.tracker.remove_run_id(run_id)


class WorkflowPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self) -> None:
        if not self.parsed_model.config.python_job_config:
            raise ValueError(
                "python_job_config is required for the `python_job_config` submission method."
            )

    def build_submitter(self) -> PythonSubmitter:
        notebook_uploader = PythonNotebookUploader(
            self.api_client,
            self.parsed_model.catalog,
            self.parsed_model.schema_,
            self.parsed_model.identifier,
        )
        config_compiler = PythonWorkflowConfigCompiler(self.parsed_model)
        workflow_creater = PythonWorkflowCreater(self.api_client.workflows)
        job_grants = {}
        if self.parsed_model.config.python_job_config:
            job_grants = self.parsed_model.config.python_job_config.grants
        return PythonNotebookWorkflowSubmitter(
            self.api_client,
            self.tracker,
            notebook_uploader,
            config_compiler,
            PythonPermissionBuilder(),
            workflow_creater,
            job_grants,
        )
