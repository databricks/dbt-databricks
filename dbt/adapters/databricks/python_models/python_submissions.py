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
    """Interface for submitting Python models to run on Databricks."""

    @abstractmethod
    def submit(self, compiled_code: str) -> None:
        """Submit the compiled code to Databricks."""
        pass


class BaseDatabricksHelper(PythonJobHelper):
    """Base helper for python models on Databricks."""

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

    def validate_config(self) -> None:
        """Perform any validation required to ensure submission method can proceed."""
        pass

    @abstractmethod
    def build_submitter(self) -> PythonSubmitter:
        """
        Since we don't own instantiation of the Helper, we construct the submitter here,
        after validation.
        """
        pass

    def submit(self, compiled_code: str) -> None:
        self.command_submitter.submit(compiled_code)


class PythonCommandSubmitter(PythonSubmitter):
    """Submitter for Python models using the Command API."""

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
    """Uploads a compiled Python model as a notebook to the Databricks workspace."""

    def __init__(
        self, api_client: DatabricksApiClient, catalog: str, schema: str, identifier: str
    ) -> None:
        self.api_client = api_client
        self.catalog = catalog
        self.schema = schema
        self.identifier = identifier

    @staticmethod
    def create(
        api_client: DatabricksApiClient, parsed_model: ParsedPythonModel
    ) -> "PythonNotebookUploader":
        return PythonNotebookUploader(
            api_client,
            parsed_model.catalog,
            parsed_model.schema_,
            parsed_model.identifier,
        )

    def upload(self, compiled_code: str) -> str:
        """Upload the compiled code to the Databricks workspace."""
        workdir = self.api_client.workspace.create_python_model_dir(self.catalog, self.schema)
        file_path = f"{workdir}{self.identifier}"
        self.api_client.workspace.upload_notebook(file_path, compiled_code)
        return file_path


@dataclass(frozen=True)
class PythonJobDetails:
    """Details required to submit a Python job run to Databricks."""

    run_name: str
    job_spec: Dict[str, Any]
    additional_job_config: Dict[str, Any]


class PythonPermissionBuilder:
    """Class for building access control list for Python jobs."""

    def __init__(
        self,
        api_client: DatabricksApiClient,
        job_grants: Dict[str, List[Dict[str, Any]]],
        acls: List[Dict[str, str]],
    ) -> None:
        self.api_client = api_client
        self.job_grants = job_grants
        self.acls = acls

    @staticmethod
    def create(
        api_client: DatabricksApiClient, parsed_model: ParsedPythonModel
    ) -> "PythonPermissionBuilder":
        if parsed_model.config.python_job_config:
            job_grants = parsed_model.config.python_job_config.grants
        else:
            job_grants = {}

        return PythonPermissionBuilder(
            api_client, job_grants, parsed_model.config.access_control_list or []
        )

    def _get_job_owner_for_config(self) -> Tuple[str, str]:
        """Get the owner of the job (and type) for the access control list."""
        curr_user = self.api_client.curr_user.get_username()
        is_service_principal = self.api_client.curr_user.is_service_principal(curr_user)

        source = "service_principal_name" if is_service_principal else "user_name"
        return curr_user, source

    def build_job_permissions(self) -> List[Dict[str, Any]]:
        """Build the access control list for the job."""

        access_control_list = []
        owner, permissions_attribute = self._get_job_owner_for_config()
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

        return access_control_list + self.acls


class PythonLibraryConfigurer:
    """Configures the libraries component for a Python job."""

    @staticmethod
    def get_library_config(
        packages: List[str],
        index_url: Optional[str],
        additional_libraries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Update the job configuration with the required libraries."""

        libraries = []

        for package in packages:
            if index_url:
                libraries.append({"pypi": {"package": package, "repo": index_url}})
            else:
                libraries.append({"pypi": {"package": package}})

        for library in additional_libraries:
            libraries.append(library)

        return {"libraries": libraries}


class PythonJobConfigCompiler:
    """Compiles a Python model into a job configuration for Databricks."""

    def __init__(
        self,
        api_client: DatabricksApiClient,
        permission_builder: PythonPermissionBuilder,
        run_name: str,
        cluster_spec: Dict[str, Any],
        additional_job_settings: Dict[str, Any],
    ) -> None:
        self.api_client = api_client
        self.permission_builder = permission_builder
        self.run_name = run_name
        self.cluster_spec = cluster_spec
        self.additional_job_settings = additional_job_settings

    @staticmethod
    def create(
        api_client: DatabricksApiClient,
        parsed_model: ParsedPythonModel,
        cluster_spec: Dict[str, Any],
    ) -> "PythonJobConfigCompiler":
        permission_builder = PythonPermissionBuilder.create(api_client, parsed_model)
        packages = parsed_model.config.packages
        index_url = parsed_model.config.index_url
        additional_libraries = parsed_model.config.additional_libs
        library_config = PythonLibraryConfigurer.get_library_config(
            packages, index_url, additional_libraries
        )
        cluster_spec.update(library_config)
        if parsed_model.config.python_job_config:
            additional_job_settings = parsed_model.config.python_job_config.dict()
        else:
            additional_job_settings = {}

        return PythonJobConfigCompiler(
            api_client,
            permission_builder,
            parsed_model.run_name,
            cluster_spec,
            additional_job_settings,
        )

    def compile(self, path: str) -> PythonJobDetails:

        job_spec: Dict[str, Any] = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": path,
            },
        }
        job_spec.update(self.cluster_spec)  # updates 'new_cluster' config

        additional_job_config = self.additional_job_settings
        access_control_list = self.permission_builder.build_job_permissions()
        if access_control_list:
            job_spec["access_control_list"] = access_control_list

        return PythonJobDetails(self.run_name, job_spec, additional_job_config)


class PythonNotebookSubmitter(PythonSubmitter):
    """Submitter for Python models using the Job API."""

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

    @staticmethod
    def create(
        api_client: DatabricksApiClient,
        tracker: PythonRunTracker,
        parsed_model: ParsedPythonModel,
        cluster_spec: Dict[str, Any],
    ) -> "PythonNotebookSubmitter":
        notebook_uploader = PythonNotebookUploader.create(api_client, parsed_model)
        config_compiler = PythonJobConfigCompiler.create(
            api_client,
            parsed_model,
            cluster_spec,
        )
        return PythonNotebookSubmitter(api_client, tracker, notebook_uploader, config_compiler)

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
    """Top level helper for Python models using job runs on a job cluster."""

    @override
    def build_submitter(self) -> PythonSubmitter:
        return PythonNotebookSubmitter.create(
            self.api_client,
            self.tracker,
            self.parsed_model,
            {"new_cluster": self.parsed_model.config.job_cluster_config},
        )

    @override
    def validate_config(self) -> None:
        if not self.parsed_model.config.job_cluster_config:
            raise ValueError(
                "`job_cluster_config` is required for the `job_cluster` submission method."
            )


class AllPurposeClusterPythonJobHelper(BaseDatabricksHelper):
    """
    Top level helper for Python models using job runs or Command API on an all-purpose cluster.
    """

    @override
    def build_submitter(self) -> PythonSubmitter:
        config = self.parsed_model.config
        if config.create_notebook:
            return PythonNotebookSubmitter.create(
                self.api_client,
                self.tracker,
                self.parsed_model,
                {"existing_cluster_id": self.cluster_id},
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
    """Top level helper for Python models using job runs on a serverless cluster."""

    def build_submitter(self) -> PythonSubmitter:
        return PythonNotebookSubmitter.create(self.api_client, self.tracker, self.parsed_model, {})


class PythonWorkflowConfigCompiler:
    """Compiles a Python model into a workflow configuration for Databricks."""

    def __init__(
        self,
        task_settings: Dict[str, Any],
        workflow_spec: Dict[str, Any],
        existing_job_id: str,
        post_hook_tasks: List[Dict[str, Any]],
    ) -> None:
        self.task_settings = task_settings
        self.existing_job_id = existing_job_id
        self.workflow_spec = workflow_spec
        self.post_hook_tasks = post_hook_tasks

    @staticmethod
    def create(parsed_model: ParsedPythonModel) -> "PythonWorkflowConfigCompiler":
        cluster_settings = PythonWorkflowConfigCompiler.cluster_settings(parsed_model)
        config = parsed_model.config
        if config.python_job_config:
            cluster_settings.update(config.python_job_config.additional_task_settings)
            workflow_spec = config.python_job_config.dict()
            workflow_spec["name"] = PythonWorkflowConfigCompiler.workflow_name(parsed_model)
            existing_job_id = config.python_job_config.existing_job_id
            post_hook_tasks = config.python_job_config.post_hook_tasks
            return PythonWorkflowConfigCompiler(
                cluster_settings, workflow_spec, existing_job_id, post_hook_tasks
            )
        else:
            return PythonWorkflowConfigCompiler(cluster_settings, {}, "", [])

    @staticmethod
    def workflow_name(parsed_model: ParsedPythonModel) -> str:
        name: Optional[str] = None
        if parsed_model.config.python_job_config:
            name = parsed_model.config.python_job_config.name
        return (
            name or f"dbt__{parsed_model.catalog}-{parsed_model.schema_}-{parsed_model.identifier}"
        )

    @staticmethod
    def cluster_settings(parsed_model: ParsedPythonModel) -> Dict[str, Any]:
        config = parsed_model.config
        job_cluster_config = config.job_cluster_config

        cluster_settings: Dict[str, Any] = {}
        if job_cluster_config:
            cluster_settings["new_cluster"] = job_cluster_config
        elif config.cluster_id:
            cluster_settings["existing_cluster_id"] = config.cluster_id

        return cluster_settings

    def compile(self, path: str) -> Tuple[Dict[str, Any], str]:
        notebook_task = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": path,
                "source": "WORKSPACE",
            },
        }
        notebook_task.update(self.task_settings)

        self.workflow_spec["tasks"] = [notebook_task] + self.post_hook_tasks
        return self.workflow_spec, self.existing_job_id


class PythonWorkflowCreater:
    """Manages the creation or updating of a workflow job on Databricks."""

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
    """Submitter for Python models using the Workflow API."""

    def __init__(
        self,
        api_client: DatabricksApiClient,
        tracker: PythonRunTracker,
        uploader: PythonNotebookUploader,
        config_compiler: PythonWorkflowConfigCompiler,
        permission_builder: PythonPermissionBuilder,
        workflow_creater: PythonWorkflowCreater,
    ) -> None:
        self.api_client = api_client
        self.tracker = tracker
        self.uploader = uploader
        self.config_compiler = config_compiler
        self.permission_builder = permission_builder
        self.workflow_creater = workflow_creater

    @staticmethod
    def create(
        api_client: DatabricksApiClient, tracker: PythonRunTracker, parsed_model: ParsedPythonModel
    ) -> "PythonNotebookWorkflowSubmitter":
        uploader = PythonNotebookUploader.create(api_client, parsed_model)
        config_compiler = PythonWorkflowConfigCompiler.create(parsed_model)
        permission_builder = PythonPermissionBuilder.create(api_client, parsed_model)
        workflow_creater = PythonWorkflowCreater(api_client.workflows)
        return PythonNotebookWorkflowSubmitter(
            api_client, tracker, uploader, config_compiler, permission_builder, workflow_creater
        )

    @override
    def submit(self, compiled_code: str) -> None:
        file_path = self.uploader.upload(compiled_code)

        workflow_config, existing_job_id = self.config_compiler.compile(file_path)
        job_id = self.workflow_creater.create_or_update(workflow_config, existing_job_id)

        access_control_list = self.permission_builder.build_job_permissions()
        self.api_client.workflow_permissions.put(job_id, access_control_list)

        run_id = self.api_client.workflows.run(job_id, enable_queueing=True)
        self.tracker.insert_run_id(run_id)

        try:
            self.api_client.job_runs.poll_for_completion(run_id)
        finally:
            self.tracker.remove_run_id(run_id)


class WorkflowPythonJobHelper(BaseDatabricksHelper):
    """Top level helper for Python models using workflow jobs on Databricks."""

    @override
    def build_submitter(self) -> PythonSubmitter:
        return PythonNotebookWorkflowSubmitter.create(
            self.api_client, self.tracker, self.parsed_model
        )
