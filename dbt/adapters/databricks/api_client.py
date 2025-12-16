import base64
import random
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from dbt_common.exceptions import DbtRuntimeError

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    CommandStatus,
    CommandStatusResponse,
)
from databricks.sdk.service.compute import Language as ComputeLanguage
from databricks.sdk.service.iam import AccessControlRequest
from databricks.sdk.service.jobs import (
    JobAccessControlRequest,
    JobSettings,
    QueueSettings,
    Run,
    SubmitTask,
)
from databricks.sdk.service.pipelines import PipelineState
from databricks.sdk.service.workspace import ImportFormat, Language
from dbt.adapters.databricks import utils
from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.credentials import (
    DatabricksCredentialManager,
    DatabricksCredentials,
)
from dbt.adapters.databricks.logging import logger

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
USER_AGENT = f"dbt-databricks/{version}"
LIBRARY_VALID_STATUSES = {"INSTALLED", "RESTORED", "SKIPPED"}


class LibraryApi:
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def all_libraries_installed(self, cluster_id: str) -> bool:
        status = self.get_cluster_libraries_status(cluster_id)
        if "library_statuses" in status:
            return all(
                library["status"] in LIBRARY_VALID_STATUSES
                for library in status["library_statuses"]
            )
        else:
            return True

    def get_cluster_libraries_status(self, cluster_id: str) -> dict[str, Any]:
        try:
            library_statuses = list(
                self.workspace_client.libraries.cluster_status(cluster_id=cluster_id)
            )
            return self._convert_library_statuses_to_legacy_format(library_statuses)
        except Exception as e:
            raise DbtRuntimeError(f"Error getting status of libraries of a cluster.\n {e}")

    def _convert_library_statuses_to_legacy_format(self, library_statuses: list) -> dict[str, Any]:
        return {
            "library_statuses": [
                {"status": lib_status.status.value if lib_status.status else "UNKNOWN"}
                for lib_status in library_statuses
            ]
        }


class ClusterApi:
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        libraries: LibraryApi,
        max_cluster_start_time: int = 900,
    ):
        self.workspace_client = workspace_client
        self.max_cluster_start_time = max_cluster_start_time
        self.libraries = libraries

    def status(self, cluster_id: str) -> str:
        try:
            cluster = self.workspace_client.clusters.get(cluster_id=cluster_id)
            logger.debug(f"Cluster status: {cluster.state}")
            return cluster.state.value if cluster.state else ""
        except Exception as e:
            raise DbtRuntimeError(f"Error getting status of cluster: {e}")

    def wait_for_cluster(self, cluster_id: str) -> None:
        start_time = time.time()

        while time.time() - start_time < self.max_cluster_start_time:
            status_response = self.status(cluster_id)

            if status_response != "RUNNING":
                logger.debug("Waiting for cluster to start")
                time.sleep(5)
                continue

            libraries_status = self.libraries.get_cluster_libraries_status(cluster_id)
            if not self.libraries.all_libraries_installed(cluster_id):
                logger.debug(f"Waiting for all libraries to be installed: {libraries_status}")
                time.sleep(5)
                continue

            return

        raise DbtRuntimeError(
            f"Cluster {cluster_id} restart timed out after {self.max_cluster_start_time} seconds"
        )

    def start(self, cluster_id: str) -> None:
        """Send the start command and poll for the cluster status until it shows "Running"

        Raise an exception if the restart exceeds our timeout.
        """
        try:
            self._start_cluster_if_needed(cluster_id)
            self.wait_for_cluster(cluster_id)
        except Exception as e:
            self._handle_start_cluster_error(cluster_id, e)

    def _start_cluster_if_needed(self, cluster_id: str) -> None:
        current_status = self.status(cluster_id)
        if current_status in ["RUNNING", "PENDING"]:
            logger.debug("Cluster already running or pending, waiting for it to be ready")
        else:
            self.workspace_client.clusters.start(cluster_id=cluster_id)
            logger.debug(f"Started cluster {cluster_id}")

    def _handle_start_cluster_error(self, cluster_id: str, error: Exception) -> None:
        if self.status(cluster_id) not in ["RUNNING", "PENDING"]:
            raise DbtRuntimeError(f"Error starting cluster: {error}")
        else:
            logger.debug("Presuming race condition, waiting for cluster to start")


class CommandContextApi:
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        cluster_api: ClusterApi,
        library_api: LibraryApi,
    ):
        self.workspace_client = workspace_client
        self.cluster_api = cluster_api
        self.library_api = library_api

    def create(self, cluster_id: str) -> str:
        self._ensure_cluster_ready(cluster_id)
        return self._create_execution_context(cluster_id)

    def _ensure_cluster_ready(self, cluster_id: str) -> None:
        current_status = self.cluster_api.status(cluster_id)

        if current_status in ["TERMINATED", "TERMINATING"]:
            logger.debug(f"Cluster {cluster_id} is not running. Attempting to restart.")
            self.cluster_api.start(cluster_id)
            logger.debug(f"Cluster {cluster_id} is now running.")
        elif current_status != "RUNNING" or not self.library_api.all_libraries_installed(
            cluster_id
        ):
            self.cluster_api.wait_for_cluster(cluster_id)

    def _create_execution_context(self, cluster_id: str, max_retries: int = 5) -> str:
        """Create execution context with retry logic for transient failures.

        Args:
            cluster_id: The cluster ID to create the context on
            max_retries: Maximum number of retry attempts (default: 5)

        Returns:
            The execution context ID

        Raises:
            DbtRuntimeError: If context creation fails after all retries
        """
        last_error = None
        for attempt in range(max_retries):
            context_id = None
            try:
                # Use SDK to create execution context - returns a Wait object
                # The Wait object provides context_id immediately, but we need to call result()
                # to wait for the context to reach RUNNING state
                waiter = self.workspace_client.command_execution.create(
                    cluster_id=cluster_id,
                    language=ComputeLanguage.PYTHON,
                )

                # Get context_id immediately (available before waiting)
                context_id = waiter.context_id
                if context_id is None:
                    raise DbtRuntimeError(
                        "Failed to create execution context: no context ID returned"
                    )

                logger.debug(f"Execution context {context_id} created, waiting for RUNNING state")

                # Now wait for the context to reach RUNNING state
                # This is where it may fail with ContextStatus.ERROR
                waiter.result()

                logger.info(f"Execution context {context_id} reached RUNNING state")
                return context_id
            except Exception as e:
                last_error = e
                error_msg = str(e).lower()

                # Log full exception details for debugging
                logger.debug(
                    f"Execution context {context_id or 'unknown'} creation exception: "
                    f"type={type(e).__name__}, message={e}"
                )

                # Retry on transient errors (resource contention, temporary failures)
                # ContextStatus.ERROR can occur when cluster is under heavy load
                if "contextstatus.error" in error_msg or "failed to reach running" in error_msg:
                    if attempt < max_retries - 1:
                        # If we have a context_id, try to destroy it before retrying
                        if context_id:
                            try:
                                logger.debug(f"Destroying failed context {context_id}")
                                self.workspace_client.command_execution.destroy(
                                    cluster_id=cluster_id, context_id=context_id
                                )
                            except Exception as cleanup_error:
                                logger.debug(
                                    f"Failed to destroy context {context_id}: {cleanup_error}"
                                )

                        # Exponential backoff with jitter: base 2^attempt + random 0-1s
                        # This helps prevent thundering herd when many contexts retry at once
                        base_wait = 2**attempt  # 1s, 2s, 4s, 8s, 16s
                        jitter = random.random()  # 0-1 second
                        wait_time = base_wait + jitter
                        logger.warning(
                            f"Execution context creation failed "
                            f"(attempt {attempt + 1}/{max_retries}), "
                            f"retrying in {wait_time:.1f}s: {e}"
                        )
                        time.sleep(wait_time)
                        continue

                # Non-retryable error or final attempt - raise immediately
                raise DbtRuntimeError(f"Error creating an execution context.\n {e}")

        # If we exhausted all retries
        raise DbtRuntimeError(
            f"Error creating an execution context after {max_retries} attempts.\n {last_error}"
        )

    def destroy(self, cluster_id: str, context_id: str) -> None:
        try:
            self.workspace_client.command_execution.destroy(
                cluster_id=cluster_id, context_id=context_id
            )
            logger.debug(f"Destroyed execution context {context_id} on cluster {cluster_id}")
        except Exception as e:
            raise DbtRuntimeError(f"Error deleting an execution context.\n {e}")


class FolderApi(ABC):
    @abstractmethod
    def get_folder(self, catalog: str, schema: str) -> str:
        pass


# Use this for now to not break users
class SharedFolderApi(FolderApi):
    def get_folder(self, _: str, schema: str) -> str:
        return f"/Shared/dbt_python_models/{schema}/"


class CurrUserApi:
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client
        self._user: Optional[str] = None

    def get_username(self) -> str:
        if self._user:
            return self._user

        try:
            current_user = self.workspace_client.current_user.me()
            username = current_user.user_name
            self._user = username
            return username or ""
        except Exception as e:
            raise DbtRuntimeError(f"Error getting current user: {e}")

    def is_service_principal(self, username: str) -> bool:
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(uuid_pattern, username, re.IGNORECASE))


# Switch to this as part of 2.0.0 release
class UserFolderApi(FolderApi):
    def __init__(self, user_api: CurrUserApi):
        self.user_api = user_api

    def get_folder(self, catalog: str, schema: str) -> str:
        username = self.user_api.get_username()
        folder = f"/Users/{username}/dbt_python_models/{catalog}/{schema}/"
        logger.debug(f"Using python model folder '{folder}'")

        return folder


class WorkspaceApi:
    def __init__(self, workspace_client: WorkspaceClient, folder_api: FolderApi):
        self.workspace_client = workspace_client
        self.user_api = folder_api

    def create_python_model_dir(self, catalog: str, schema: str) -> str:
        folder = self.user_api.get_folder(catalog, schema)

        try:
            self.workspace_client.workspace.mkdirs(path=folder)
            return folder
        except Exception as e:
            raise DbtRuntimeError(f"Error creating work_dir for python notebooks: {e}")

    def upload_notebook(self, path: str, compiled_code: str) -> None:
        b64_encoded_content = base64.b64encode(compiled_code.encode()).decode()
        try:
            self.workspace_client.workspace.import_(
                path=path,
                content=b64_encoded_content,
                language=Language.PYTHON,
                overwrite=True,
                format=ImportFormat.SOURCE,
            )
        except Exception as e:
            raise DbtRuntimeError(f"Error creating python notebook: {e}")

    def get_object_id(self, path: str) -> int:
        """
        Get the workspace object ID for the specified path
        """
        try:
            object_info = self.workspace_client.workspace.get_status(path=path)
            object_id = object_info.object_id

            if not object_id:
                raise DbtRuntimeError(f"No object_id found for path {path}")

            return object_id
        except Exception as e:
            raise DbtRuntimeError(f"Error getting workspace object ID for {path}: {e}")


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommandExecution:
    command_id: str
    context_id: str
    cluster_id: str

    def model_dump(self) -> dict[str, Any]:
        return {
            "commandId": self.command_id,
            "contextId": self.context_id,
            "clusterId": self.cluster_id,
        }


class CommandApi:
    def __init__(self, workspace_client: WorkspaceClient, polling_interval: int, timeout: int):
        self.workspace_client = workspace_client
        self.polling_interval = polling_interval
        self.timeout = timeout

    def execute(self, cluster_id: str, context_id: str, command: str) -> CommandExecution:
        try:
            # Use SDK to execute command - returns a Wait object immediately
            # The command_id is available via __getattr__ without calling result()
            # We don't call result() because that would wait for execution to finish,
            # and we want to use our own timeout via poll_for_completion()
            waiter = self.workspace_client.command_execution.execute(
                cluster_id=cluster_id,
                context_id=context_id,
                language=ComputeLanguage.PYTHON,  # SUBMISSION_LANGUAGE was "python"
                command=command,
            )

            # Extract command_id from the waiter without blocking
            # The SDK provides this immediately in the kwargs
            command_id = waiter.command_id
            if command_id is None:
                raise DbtRuntimeError("Failed to execute command: no command ID returned")
            logger.debug(f"Command executed with id={command_id}")
            return CommandExecution(
                command_id=command_id, cluster_id=cluster_id, context_id=context_id
            )
        except Exception as e:
            raise DbtRuntimeError(f"Error creating a command.\n {e}")

    def cancel(self, command: CommandExecution) -> None:
        logger.debug(f"Cancelling command {command}")
        try:
            self.workspace_client.command_execution.cancel(
                cluster_id=command.cluster_id,
                context_id=command.context_id,
                command_id=command.command_id,
            )
        except Exception as e:
            raise DbtRuntimeError(f"Cancel command {command} failed.\n {e}")

    def poll_for_completion(self, command: CommandExecution) -> None:
        start = time.time()
        terminal_states = {CommandStatus.FINISHED, CommandStatus.ERROR, CommandStatus.CANCELLED}

        while True:
            if time.time() - start > self.timeout:
                raise DbtRuntimeError("Command execution timed out")

            try:
                status_response = self.workspace_client.command_execution.command_status(
                    cluster_id=command.cluster_id,
                    context_id=command.context_id,
                    command_id=command.command_id,
                )

                if status_response.status in terminal_states:
                    self._handle_terminal_state(status_response)
                    break

                time.sleep(self.polling_interval)

            except Exception as e:
                raise DbtRuntimeError(f"Error polling for command completion.\n {e}")

    def _handle_terminal_state(self, status_response: CommandStatusResponse) -> None:
        if status_response.status == CommandStatus.CANCELLED:
            raise DbtRuntimeError("Command was cancelled")
        elif status_response.status == CommandStatus.ERROR:
            error_msg = self._extract_error_message(status_response)
            raise DbtRuntimeError(
                f"Python model run ended in state {status_response.status.value} "
                f"with state_message\n{error_msg}"
            )
        elif status_response.status == CommandStatus.FINISHED:
            self._check_for_execution_error(status_response)

    def _extract_error_message(self, status_response: CommandStatusResponse) -> str:
        if status_response.results and hasattr(status_response.results, "data"):
            data = status_response.results.data
            return str(data) if data is not None else "Unknown error"
        return "Unknown error"

    def _check_for_execution_error(self, status_response: CommandStatusResponse) -> None:
        if status_response.results:
            result_type = getattr(status_response.results, "result_type", None)

            # Check for error result type (could be enum or string)
            is_error = False
            if result_type:
                # Handle both string and enum values
                if hasattr(result_type, "value"):
                    is_error = result_type.value == "error"
                else:
                    is_error = result_type == "error"

            if is_error:
                error_cause = getattr(status_response.results, "cause", "Unknown error")
                raise DbtRuntimeError(
                    f"Python model failed with traceback as:\n{utils.remove_ansi(error_cause)}"
                )


class JobRunsApi:
    def __init__(self, workspace_client: WorkspaceClient, polling_interval: int, timeout: int):
        self.workspace_client = workspace_client
        self.timeout = timeout
        self.polling_interval = polling_interval

    def submit(
        self, run_name: str, job_spec: dict[str, Any], **additional_job_settings: dict[str, Any]
    ) -> str:
        logger.debug(
            f"Submitting job with run_name={run_name} and job_spec={job_spec}"
            " and additional_job_settings={additional_job_settings}"
        )
        try:
            tasks = self._convert_job_spec_to_tasks(job_spec)
            submit_result = self._submit_job_to_databricks(
                run_name, tasks, job_spec, additional_job_settings
            )
            return self._extract_run_id(submit_result)
        except Exception as e:
            raise DbtRuntimeError(f"Error creating python run.\n {e}")

    def _convert_job_spec_to_tasks(self, job_spec: dict[str, Any]) -> list:
        # Convert job_spec to be compatible with SubmitTask
        task_spec = job_spec.copy()

        # Convert cluster_id to existing_cluster_id if present
        if "cluster_id" in task_spec:
            task_spec["existing_cluster_id"] = task_spec.pop("cluster_id")

        # Remove fields that are not part of SubmitTask and should be handled separately
        task_spec.pop("access_control_list", None)  # Handled via permissions API
        task_spec.pop("queue", None)  # Handled in submission parameters

        return [SubmitTask.from_dict(task_spec)]

    def _submit_job_to_databricks(
        self,
        run_name: str,
        tasks: list,
        job_spec: dict[str, Any],
        additional_job_settings: dict[str, Any],
    ) -> Any:
        # Extract submission-level parameters from job_spec
        submission_params: dict[str, Any] = {"tasks": tasks}

        # Handle queue settings
        if "queue" in job_spec:
            submission_params["queue"] = QueueSettings.from_dict(job_spec["queue"])

        # Handle access control list
        if "access_control_list" in job_spec:
            submission_params["access_control_list"] = [
                JobAccessControlRequest.from_dict(acl) for acl in job_spec["access_control_list"]
            ]

        # Add any additional job settings
        submission_params.update(additional_job_settings)

        # Filter out parameters that the Databricks SDK doesn't expect
        # The SDK submit() method doesn't accept 'name' or 'run_name' in the request body
        filtered_params = {
            k: v for k, v in submission_params.items() if k not in ["name", "run_name"]
        }

        return self.workspace_client.jobs.submit(run_name=run_name, **filtered_params)

    def _extract_run_id(self, submit_result: Any) -> str:
        run_id = str(submit_result.run_id)
        logger.info(f"Job submitted successfully with run_id={run_id}")
        return run_id

    def get_run_info(self, run_id: str) -> dict[str, Any]:
        try:
            run = self.workspace_client.jobs.get_run(run_id=int(run_id))
            return self._convert_run_to_legacy_format(run)
        except Exception as e:
            raise DbtRuntimeError(f"Error getting run info.\n {e}")

    def _convert_run_to_legacy_format(self, run: Any) -> dict[str, Any]:
        return run.as_dict()

    def get_job_id_from_run_id(self, run_id: str) -> str:
        run_info = self.get_run_info(run_id)
        job_id = run_info.get("job_id")

        if not job_id:
            raise DbtRuntimeError(f"Could not get job_id from run_id {run_id}")

        return str(job_id)

    def poll_for_completion(self, run_id: str) -> None:
        start = time.time()
        terminal_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

        while True:
            if time.time() - start > self.timeout:
                raise DbtRuntimeError("Python model run timed out")

            try:
                run = self.workspace_client.jobs.get_run(run_id=int(run_id))
                state = run.state

                if not state:
                    time.sleep(self.polling_interval)
                    continue

                life_cycle_state = state.life_cycle_state.value if state.life_cycle_state else ""

                if life_cycle_state in terminal_states:
                    # Handle the terminal state
                    self._handle_terminal_state(run)
                    break

                time.sleep(self.polling_interval)

            except Exception as e:
                raise DbtRuntimeError(f"Error polling for completion.\n {e}")

    def _handle_terminal_state(self, run: Run) -> None:
        state = run.state
        if not state:
            return

        result_state = state.result_state.value if state.result_state else None
        life_cycle_state = state.life_cycle_state.value if state.life_cycle_state else ""
        state_message = state.state_message or ""

        # Add detailed logging for debugging
        logger.debug(f"[Python Model Debug] Life cycle state: {life_cycle_state}")
        logger.debug(f"[Python Model Debug] Result state: {result_state}")

        if result_state == "CANCELED":
            raise DbtRuntimeError(f"Python model run ended in result_state {result_state}")

        if life_cycle_state != "TERMINATED":
            self._handle_non_terminated_failure(
                run, life_cycle_state, state_message, result_state or ""
            )

    def cancel(self, run_id: str) -> None:
        logger.debug(f"Cancelling run id {run_id}")
        try:
            self.workspace_client.jobs.cancel_run(run_id=int(run_id))
        except Exception as e:
            raise DbtRuntimeError(f"Cancel run {run_id} failed.\n {e}")

    def _handle_non_terminated_failure(
        self, run: Any, life_cycle_state: str, state_message: str, result_state: str
    ) -> None:
        try:
            tasks = run.tasks or []
            self._log_task_debug_info(tasks)

            if tasks:
                task_id = tasks[0].run_id
                error_msg, error_trace = self._extract_task_error_details(task_id)
                self._raise_detailed_python_model_error(run.run_id, task_id, error_msg, error_trace)

        except Exception as e:
            if isinstance(e, DbtRuntimeError):
                raise e
            else:
                logger.debug(f"[Python Model Debug] Exception during error extraction: {e}")
                raise DbtRuntimeError(
                    f"Python model run ended in state {life_cycle_state} "
                    f"(run_id: {run.run_id})\n"
                    f"State message: {state_message}\n"
                    f"Result state: {result_state}"
                )

    def _log_task_debug_info(self, tasks: list) -> None:
        logger.debug(f"[Python Model Debug] Tasks in response: {len(tasks)}")
        for i, task in enumerate(tasks):
            logger.debug(f"[Python Model Debug] Task {i}: {task}")

    def _extract_task_error_details(self, task_id: Any) -> tuple[str, str]:
        if task_id:
            logger.debug(f"[Python Model Debug] Getting output for task_id: {task_id}")
            run_output = self.workspace_client.jobs.get_run_output(run_id=task_id)
            error_msg = run_output.error or "No error message available"
            error_trace = utils.remove_ansi(run_output.error_trace or "")

            if error_trace:
                logger.debug(f"[Python Model Debug] Error trace found: {error_trace[:500]}...")

            return error_msg, error_trace
        else:
            return "No error message available", ""

    def _raise_detailed_python_model_error(
        self, run_id: Any, task_id: Any, error_msg: str, error_trace: str
    ) -> None:
        raise DbtRuntimeError(
            f"Python model failed (run_id: {run_id}, task_id: {task_id})\n"
            "Traceback:\n"
            "(Note that the line number here does not "
            "match the line number in your code due to dbt templating)\n"
            f"{error_msg}\n"
            f"{error_trace}"
        )


class JobPermissionsApi:
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def put(self, job_id: str, access_control_list: list[dict[str, Any]]) -> None:
        try:
            # Convert dict access control list to SDK AccessControlRequest objects
            access_control_requests = [
                AccessControlRequest.from_dict(acl) for acl in access_control_list
            ]

            result = self.workspace_client.permissions.set(
                request_object_type="jobs",
                request_object_id=job_id,
                access_control_list=access_control_requests,
            )
            logger.debug(f"Workflow permissions update response={result}")
        except Exception as e:
            raise DbtRuntimeError(f"Error updating Databricks workflow.\n {e}")

    def patch(self, job_id: str, access_control_list: list[dict[str, Any]]) -> None:
        try:
            # Convert dict access control list to SDK AccessControlRequest objects
            access_control_requests = [
                AccessControlRequest.from_dict(acl) for acl in access_control_list
            ]

            result = self.workspace_client.permissions.update(
                request_object_type="jobs",
                request_object_id=job_id,
                access_control_list=access_control_requests,
            )
            logger.debug(f"Workflow permissions update response={result}")
        except Exception as e:
            raise DbtRuntimeError(f"Error updating Databricks workflow.\n {e}")

    def get(self, job_id: str) -> dict[str, Any]:
        try:
            result = self.workspace_client.permissions.get(
                request_object_type="jobs", request_object_id=job_id
            )
            # Convert SDK ObjectPermissions to dict for backward compatibility
            return result.as_dict()
        except Exception as e:
            raise DbtRuntimeError(f"Error fetching Databricks workflow permissions.\n {e}")


class NotebookPermissionsApi:
    def __init__(self, workspace_client: WorkspaceClient, workspace_api: "WorkspaceApi"):
        self.workspace_client = workspace_client
        self.workspace_api = workspace_api

    def put(self, notebook_path: str, access_control_list: list[dict[str, Any]]) -> None:
        try:
            # Get the notebook's object ID from workspace API
            object_id = self.workspace_api.get_object_id(notebook_path)

            # Convert dict access control list to SDK AccessControlRequest objects
            access_control_requests = [
                AccessControlRequest.from_dict(acl) for acl in access_control_list
            ]

            logger.debug(
                f"Setting notebook permissions for path={notebook_path}, object_id={object_id}"
            )
            logger.debug(f"Permission request: {access_control_list}")

            result = self.workspace_client.permissions.set(
                request_object_type="notebooks",
                request_object_id=str(object_id),
                access_control_list=access_control_requests,
            )
            logger.debug(f"Notebook permissions update response={result}")
        except Exception as e:
            raise DbtRuntimeError(
                f"Error updating Databricks notebook permissions.\npath={notebook_path}, error: {e}"
            )

    def get(self, notebook_path: str) -> dict[str, Any]:
        try:
            # Get the notebook's object ID from workspace API
            object_id = self.workspace_api.get_object_id(notebook_path)

            logger.debug(
                f"Getting notebook permissions for path={notebook_path}, object_id={object_id}"
            )

            result = self.workspace_client.permissions.get(
                request_object_type="notebooks",
                request_object_id=str(object_id),
            )

            # Convert SDK response back to dict format for backward compatibility
            return result.as_dict()
        except Exception as e:
            raise DbtRuntimeError(
                f"Error fetching Databricks notebook permissions.\npath={notebook_path}, error: {e}"
            )


class WorkflowJobApi:
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def search_by_name(self, job_name: str) -> list[dict[str, Any]]:
        try:
            # Use SDK to list jobs by name
            jobs = list(self.workspace_client.jobs.list(name=job_name))
            # Convert SDK BaseJob objects to dict format for backward compatibility
            return [job.as_dict() for job in jobs]
        except Exception as e:
            raise DbtRuntimeError(f"Error fetching job by name.\n {e}")

    def create(self, job_spec: dict[str, Any]) -> str:
        """
        :return: the job_id
        """
        try:
            # Convert job_spec to be compatible with jobs.create
            converted_job_spec = self._convert_job_spec_for_create(job_spec)
            create_response = self.workspace_client.jobs.create(**converted_job_spec)
            job_id = str(create_response.job_id)
            logger.info(f"New workflow created with job id {job_id}")
            return job_id
        except Exception as e:
            raise DbtRuntimeError(f"Error creating Workflow.\n {e}")

    def _convert_job_spec_for_create(self, job_spec: dict[str, Any]) -> dict[str, Any]:
        """Convert job_spec to be compatible with jobs.create API."""
        converted_spec = job_spec.copy()

        # Convert tasks if present
        if "tasks" in converted_spec and converted_spec["tasks"]:
            converted_tasks = []
            for task in converted_spec["tasks"]:
                converted_task = task.copy()
                # Convert cluster_id to existing_cluster_id if present
                if "cluster_id" in converted_task:
                    converted_task["existing_cluster_id"] = converted_task.pop("cluster_id")
                converted_tasks.append(converted_task)
            converted_spec["tasks"] = converted_tasks

        return converted_spec

    def update_job_settings(self, job_id: str, job_spec: dict[str, Any]) -> None:
        try:
            # Convert job_spec to be compatible with jobs.reset
            converted_job_spec = self._convert_job_spec_for_create(job_spec)
            # Use SDK to reset job settings
            # Convert job_spec to JobSettings object
            new_settings = JobSettings.from_dict(converted_job_spec)
            self.workspace_client.jobs.reset(job_id=int(job_id), new_settings=new_settings)
            logger.debug(f"Workflow updated for job_id={job_id}")
        except Exception as e:
            raise DbtRuntimeError(f"Error updating Workflow.\n {e}")

    def run(self, job_id: str, enable_queueing: bool = True) -> str:
        try:
            # Use SDK to run job now
            queue_settings = QueueSettings(enabled=enable_queueing)

            run_result = self.workspace_client.jobs.run_now(
                job_id=int(job_id), queue=queue_settings
            )

            run_id = str(run_result.run_id)
            logger.info(f"Workflow triggered with run_id={run_id}")
            return run_id
        except Exception as e:
            raise DbtRuntimeError(f"Error triggering run for workflow.\n {e}")


class DltPipelineApi:
    def __init__(self, workspace_client: WorkspaceClient, polling_interval: int):
        self.workspace_client = workspace_client
        self.polling_interval = polling_interval

    def poll_for_completion(self, pipeline_id: str) -> None:
        try:
            # Use SDK's built-in wait method that polls until pipeline is IDLE
            self.workspace_client.pipelines.wait_get_pipeline_idle(
                pipeline_id=pipeline_id,
                timeout=timedelta(hours=1),  # 60 * 60 seconds = 1 hour timeout
            )
            logger.debug(f"Pipeline {pipeline_id} completed successfully")
        except Exception as e:
            # Get more detailed error information
            try:
                pipeline = self.workspace_client.pipelines.get(pipeline_id=pipeline_id)
                if pipeline.state == PipelineState.FAILED:
                    # Try to get error details from the latest update
                    if pipeline.latest_updates:
                        latest_update = pipeline.latest_updates[0]
                        update_id = latest_update.update_id
                        if update_id is not None:
                            error_msg = self.get_update_error(pipeline_id, update_id)
                            if error_msg:
                                raise DbtRuntimeError(f"Pipeline {pipeline_id} failed: {error_msg}")

                    # Fallback to generic error
                    raise DbtRuntimeError(f"Pipeline {pipeline_id} failed: {str(e)}")
                else:
                    raise DbtRuntimeError(f"Pipeline {pipeline_id} polling failed: {str(e)}")
            except DbtRuntimeError:
                # Re-raise DbtRuntimeError as-is (don't wrap it)
                raise
            except Exception:
                # For other exceptions, use the original error
                raise DbtRuntimeError(f"Pipeline {pipeline_id} failed: {str(e)}")

    def get_update_error(self, pipeline_id: str, update_id: str) -> str:
        try:
            # Get pipeline events using SDK
            events_response = self.workspace_client.pipelines.list_pipeline_events(
                pipeline_id=pipeline_id
            )

            # Filter events for the specific update and error state
            update_events = [
                event
                for event in events_response
                if (
                    event.event_type == "update_progress"
                    and event.origin
                    and event.origin.update_id == update_id
                )
            ]

            # Look for events with error information
            error_events = [
                event
                for event in update_events
                if event.error is not None
                or (
                    event.message
                    and ("error" in event.message.lower() or "fail" in event.message.lower())
                )
            ]

            if error_events:
                error_event = error_events[0]
                if error_event.error:
                    return str(error_event.error)
                elif error_event.message:
                    return error_event.message
                else:
                    return "Pipeline update failed"

            return ""
        except Exception as e:
            raise DbtRuntimeError(f"Error getting pipeline event info for {pipeline_id}: {str(e)}")


class DatabricksApiClient:
    def __init__(
        self,
        credentials: DatabricksCredentials,
        timeout: int,
        use_user_folder: bool = False,
        polling_interval: int = DEFAULT_POLLING_INTERVAL,
    ):
        workspace_client = DatabricksCredentialManager.create_from(credentials).api_client
        self.libraries = LibraryApi(workspace_client)
        self.clusters = ClusterApi(workspace_client, self.libraries)
        self.command_contexts = CommandContextApi(workspace_client, self.clusters, self.libraries)
        self.curr_user = CurrUserApi(workspace_client)
        if use_user_folder:
            self.folders: FolderApi = UserFolderApi(self.curr_user)
        else:
            self.folders = SharedFolderApi()
        self.workspace = WorkspaceApi(workspace_client, self.folders)
        self.commands = CommandApi(workspace_client, polling_interval, timeout)
        self.job_runs = JobRunsApi(workspace_client, polling_interval, timeout)
        self.workflows = WorkflowJobApi(workspace_client)
        self.workflow_permissions = JobPermissionsApi(workspace_client)
        self.notebook_permissions = NotebookPermissionsApi(workspace_client, self.workspace)
        self.dlt_pipelines = DltPipelineApi(workspace_client, polling_interval)
