import base64
from datetime import timedelta
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any
from typing import Dict

from dbt.adapters.databricks import utils
from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.credentials import BearerAuth, DatabricksCredentials
from dbt.adapters.databricks.logging import logger
from dbt_common.exceptions import DbtRuntimeError
from requests import Response
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language as ComputeLanguage
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.workspace import Language as NotebookLanguage
from databricks.sdk.service.jobs import SubmitTask

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
USER_AGENT = f"dbt-databricks/{version}"


class FolderApi(ABC):
    @abstractmethod
    def get_folder(self, catalog: str, schema: str) -> str:
        pass


# Use this for now to not break users
class SharedFolderApi(FolderApi):
    def get_folder(self, _: str, schema: str) -> str:
        logger.warning(
            f"Uploading notebook to '/Shared/dbt_python_models/{schema}/'.  "
            "Writing to '/Shared' is deprecated and will be removed in a future release.  "
            "Write to the current user's home directory by setting `user_folder_for_python: true`"
        )
        return f"/Shared/dbt_python_models/{schema}/"


# Switch to this as part of 2.0.0 release
class UserFolderApi(FolderApi):
    def __init__(self, client: WorkspaceClient):
        self.client = client
        self._user = ""

    def get_folder(self, catalog: str, schema: str) -> str:
        if not self._user:
            self._user = self.client.current_user.me().user_name or ""
            if not self._user:
                raise DbtRuntimeError("Error retrieving current user name")
        folder = f"/Users/{self._user}/dbt_python_models/{catalog}/{schema}/"
        logger.debug(f"Using python model folder '{folder}'")

        return folder


class WorkspaceApi:
    def __init__(self, client: WorkspaceClient, folder_api: FolderApi):
        self.client = client
        self.user_api = folder_api

    def create_python_model_dir(self, catalog: str, schema: str) -> str:
        folder = self.user_api.get_folder(catalog, schema)

        self.client.workspace.mkdirs(folder)
        return folder

    def upload_notebook(self, path: str, compiled_code: str) -> None:
        b64_encoded_content = base64.b64encode(compiled_code.encode()).decode()
        self.client.workspace.import_(
            path,
            content=b64_encoded_content,
            format=ImportFormat.SOURCE,
            language=NotebookLanguage.PYTHON,
            overwrite=True,
        )


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommandExecution(object):
    command_id: str
    context_id: str
    cluster_id: str

    def model_dump(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "context_id": self.context_id,
            "cluster_id": self.cluster_id,
        }


class CommandApi:
    def __init__(self, client: WorkspaceClient, timeout: int):
        self.client = client
        self.timeout = timeout

    def execute(self, cluster_id: str, command: str) -> CommandExecution:
        self.client.clusters.ensure_cluster_is_running(cluster_id)
        context = self.client.command_execution.create_and_wait(
            cluster_id=cluster_id, language=ComputeLanguage.PYTHON
        )
        try:
            waiter = self.client.command_execution.execute(
                cluster_id=cluster_id,
                command=command,
                context_id=context.id,
                language=ComputeLanguage.PYTHON,
            )

            return CommandExecution(
                command_id=waiter.command_id, cluster_id=cluster_id, context_id=context.id or ""
            )
        except Exception as e:
            if context and context.id:
                logger.debug(f"Destroying command context {context.id} due to error with execution")
                self.client.command_execution.destroy(cluster_id, context.id)
            raise e

    def cancel(self, command: CommandExecution) -> None:
        logger.debug(f"Cancelling command {command}")
        self.client.command_execution.cancel_and_wait()

    def poll_for_completion(self, command: CommandExecution) -> None:
        logger.debug(f"Waiting on command {command}")
        try:
            self.client.command_execution.wait_command_status_command_execution_finished_or_error(
                command.cluster_id,
                command.command_id,
                command.context_id,
                timedelta(seconds=self.timeout),
            )
        finally:
            logger.debug(f"Destroying command context for {command}")
            self.client.command_execution.destroy(command.cluster_id, command.context_id)


class JobRunsApi:
    def __init__(self, client: WorkspaceClient, timeout: int):
        self.client = client
        self.timeout = timeout

    def submit(self, run_name: str, job_spec: Dict[str, Any]) -> int:
        waiter = self.client.jobs.submit(run_name=run_name, tasks=[SubmitTask.from_dict(job_spec)])
        return waiter.run_id

    def poll_for_completion(self, run_id: int) -> None:
        logger.debug(f"Waiting on run {run_id}")
        self.client.jobs.wait_get_run_job_terminated_or_skipped(
            run_id, timedelta(seconds=self.timeout)
        )

    def cancel(self, run_id: int) -> None:
        logger.debug(f"Cancelling run id {run_id}")
        self.client.jobs.cancel_run_and_wait(run_id)


class DatabricksApiClient:
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        timeout: int,
        use_user_folder: bool = False,
    ):
        if use_user_folder:
            self.folders: FolderApi = UserFolderApi(workspace_client)
        else:
            self.folders = SharedFolderApi()
        self.workspace = WorkspaceApi(workspace_client, self.folders)
        self.commands = CommandApi(workspace_client, timeout)
        self.job_runs = JobRunsApi(workspace_client, timeout)
