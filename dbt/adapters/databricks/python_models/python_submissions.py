from abc import ABC, abstractmethod
from typing import Any, Optional

from dbt_common.exceptions import DbtRuntimeError
from pydantic import BaseModel
from typing_extensions import override

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.databricks.api_client import CommandExecution, DatabricksApiClient, WorkflowJobApi
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.python_models.python_config import ParsedPythonModel
from dbt.adapters.databricks.python_models.run_tracking import PythonRunTracker

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

    def __init__(self, parsed_model: dict, credentials: DatabricksCredentials) -> None:
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
        logger.debug("Submitting Python model using the Command API.")

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

    def __init__(self, api_client: DatabricksApiClient, parsed_model: ParsedPythonModel) -> None:
        self.api_client = api_client
        self.catalog = parsed_model.catalog
        self.schema = parsed_model.schema_
        self.identifier = parsed_model.identifier

    def upload(self, compiled_code: str) -> str:
        """Upload the compiled code to the Databricks workspace."""
        workdir = self.api_client.workspace.create_python_model_dir(self.catalog, self.schema)
        file_path = f"{workdir}{self.identifier}"
        self.api_client.workspace.upload_notebook(file_path, compiled_code)
        return file_path


class PythonJobDetails(BaseModel):
    """Details required to submit a Python job run to Databricks."""

    run_name: str
    job_spec: dict[str, Any]
    additional_job_config: dict[str, Any]


class PythonPermissionBuilder:
    """Class for building access control list for Python jobs."""

    def __init__(
        self,
        api_client: DatabricksApiClient,
    ) -> None:
        self.api_client = api_client

    def _get_job_owner_for_config(self) -> tuple[str, str]:
        """Get the owner of the job (and type) for the access control list."""
        curr_user = self.api_client.curr_user.get_username()
        is_service_principal = self.api_client.curr_user.is_service_principal(curr_user)

        source = "service_principal_name" if is_service_principal else "user_name"
        return curr_user, source

    @staticmethod
    def _build_job_permission(
        job_grants: list[dict[str, Any]], permission: str
    ) -> list[dict[str, Any]]:
        return [{**grant, **{"permission_level": permission}} for grant in job_grants]

    def build_job_permissions(
        self,
        job_grants: dict[str, list[dict[str, Any]]],
        acls: list[dict[str, str]],
    ) -> list[dict[str, Any]]:
        """Build the access control list for the job."""

        access_control_list = []
        owner, permissions_attribute = self._get_job_owner_for_config()
        access_control_list.append(
            {
                permissions_attribute: owner,
                "permission_level": "IS_OWNER",
            }
        )

        access_control_list.extend(
            self._build_job_permission(job_grants.get("view", []), "CAN_VIEW")
        )
        access_control_list.extend(
            self._build_job_permission(job_grants.get("run", []), "CAN_MANAGE_RUN")
        )
        access_control_list.extend(
            self._build_job_permission(job_grants.get("manage", []), "CAN_MANAGE")
        )

        return access_control_list + acls


def get_library_config(
    packages: list[str],
    index_url: Optional[str],
    additional_libraries: list[dict[str, Any]],
) -> dict[str, Any]:
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
        parsed_model: ParsedPythonModel,
        cluster_spec: dict[str, Any],
    ) -> None:
        self.api_client = api_client
        self.permission_builder = permission_builder
        self.run_name = parsed_model.run_name
        packages = parsed_model.config.packages
        index_url = parsed_model.config.index_url
        additional_libraries = parsed_model.config.additional_libs
        library_config = get_library_config(packages, index_url, additional_libraries)
        self.cluster_spec = {**cluster_spec, **library_config}
        self.job_grants = parsed_model.config.python_job_config.grants
        self.acls = parsed_model.config.access_control_list
        self.additional_job_settings = parsed_model.config.python_job_config.dict()

    def compile(self, path: str) -> PythonJobDetails:
        job_spec: dict[str, Any] = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": path,
            },
        }
        job_spec.update(self.cluster_spec)  # updates 'new_cluster' config

        additional_job_config = self.additional_job_settings
        access_control_list = self.permission_builder.build_job_permissions(
            self.job_grants, self.acls
        )
        if access_control_list:
            job_spec["access_control_list"] = access_control_list

        job_spec["queue"] = {"enabled": True}
        return PythonJobDetails(
            run_name=self.run_name, job_spec=job_spec, additional_job_config=additional_job_config
        )


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
        cluster_spec: dict[str, Any],
    ) -> "PythonNotebookSubmitter":
        notebook_uploader = PythonNotebookUploader(api_client, parsed_model)
        permission_builder = PythonPermissionBuilder(api_client)
        config_compiler = PythonJobConfigCompiler(
            api_client,
            permission_builder,
            parsed_model,
            cluster_spec,
        )
        return PythonNotebookSubmitter(api_client, tracker, notebook_uploader, config_compiler)

    @override
    def submit(self, compiled_code: str) -> None:
        logger.debug("Submitting Python model using the Job Run API.")

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

    def __init__(self, parsed_model: dict, credentials: DatabricksCredentials) -> None:
        self.credentials = credentials
        self.credentials.validate_creds()
        self.parsed_model = ParsedPythonModel(**parsed_model)

        self.api_client = DatabricksApiClient.create(
            credentials,
            self.parsed_model.config.timeout,
            self.parsed_model.config.user_folder_for_python,
        )

        config = self.parsed_model.config
        self.create_notebook = config.create_notebook
        self.cluster_id = config.cluster_id or self.credentials.extract_cluster_id(
            config.http_path or self.credentials.http_path or ""
        )
        self.validate_config()

        self.command_submitter = self.build_submitter()

    @override
    def build_submitter(self) -> PythonSubmitter:
        if self.create_notebook:
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
        task_settings: dict[str, Any],
        workflow_spec: dict[str, Any],
        existing_job_id: str,
        post_hook_tasks: list[dict[str, Any]],
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
    def cluster_settings(parsed_model: ParsedPythonModel) -> dict[str, Any]:
        config = parsed_model.config
        job_cluster_config = config.job_cluster_config

        cluster_settings: dict[str, Any] = {}
        if job_cluster_config:
            cluster_settings["new_cluster"] = job_cluster_config
        elif config.cluster_id:
            cluster_settings["existing_cluster_id"] = config.cluster_id

        return cluster_settings

    def compile(self, path: str) -> tuple[dict[str, Any], str]:
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


class PythonWorkflowCreator:
    """Manages the creation or updating of a workflow job on Databricks."""

    def __init__(self, workflows: WorkflowJobApi) -> None:
        self.workflows = workflows

    def create_or_update(
        self,
        workflow_spec: dict[str, Any],
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
            elif len(response_jobs) == 1:
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
        workflow_creater: PythonWorkflowCreator,
        job_grants: dict[str, list[dict[str, str]]],
        acls: list[dict[str, str]],
    ) -> None:
        self.api_client = api_client
        self.tracker = tracker
        self.uploader = uploader
        self.config_compiler = config_compiler
        self.permission_builder = permission_builder
        self.workflow_creater = workflow_creater
        self.job_grants = job_grants
        self.acls = acls

    @staticmethod
    def create(
        api_client: DatabricksApiClient, tracker: PythonRunTracker, parsed_model: ParsedPythonModel
    ) -> "PythonNotebookWorkflowSubmitter":
        uploader = PythonNotebookUploader(api_client, parsed_model)
        config_compiler = PythonWorkflowConfigCompiler.create(parsed_model)
        permission_builder = PythonPermissionBuilder(api_client)
        workflow_creater = PythonWorkflowCreator(api_client.workflows)
        return PythonNotebookWorkflowSubmitter(
            api_client,
            tracker,
            uploader,
            config_compiler,
            permission_builder,
            workflow_creater,
            parsed_model.config.python_job_config.grants,
            parsed_model.config.access_control_list,
        )

    @override
    def submit(self, compiled_code: str) -> None:
        logger.debug("Submitting Python model using the Workflow API.")

        file_path = self.uploader.upload(compiled_code)

        workflow_config, existing_job_id = self.config_compiler.compile(file_path)
        job_id = self.workflow_creater.create_or_update(workflow_config, existing_job_id)

        access_control_list = self.permission_builder.build_job_permissions(
            self.job_grants, self.acls
        )
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
