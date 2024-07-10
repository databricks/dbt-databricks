import base64
from datetime import timedelta
import time
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
from typing import Callable
from typing import Dict
from typing import Set

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
from databricks.sdk.service.compute import State
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.workspace import Language as NotebookLanguage

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
USER_AGENT = f"dbt-databricks/{version}"


class ClusterApi:
    def __init__(self, client: WorkspaceClient, max_cluster_start_time: int = 900):
        self.client = client
        self.max_cluster_start_time = max_cluster_start_time

    def start(self, cluster_id: str) -> str:
        details = self.client.clusters.get(cluster_id=cluster_id)
        if details.state != State.RUNNING:
            cluster = self.client.clusters.start_and_wait(
                cluster_id, timedelta(seconds=self.max_cluster_start_time)
            )
            logger.debug(f"Cluster start response={cluster.as_dict()}")

            if cluster and cluster.state:
                return cluster.state.name
            else:
                raise DbtRuntimeError(f"Error retrieving Cluster {cluster_id}")
        return details.state.name


class CommandContextApi:
    def __init__(self, client: WorkspaceClient, cluster_api: ClusterApi):
        self.client = client
        self.cluster_api = cluster_api

    def create(self, cluster_id: str) -> str:
        self.cluster_api.start(cluster_id)
        response = self.client.command_execution.create_and_wait(
            cluster_id=cluster_id, language=ComputeLanguage.PYTHON
        )

        if not response.id:
            raise DbtRuntimeError(f"Error creating an execution context: {response}.")
        return response.id

    def destroy(self, cluster_id: str, context_id: str) -> None:
        self.client.command_execution.destroy(cluster_id, context_id)


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


class PrefixSession:
    def __init__(self, session: Session, host: str, api: str):
        self.prefix = f"https://{host}{api}"
        self.session = session

    def get(
        self, suffix: str = "", json: Optional[Any] = None, params: Optional[Dict[str, Any]] = None
    ) -> Response:
        return self.session.get(f"{self.prefix}{suffix}", json=json, params=params)

    def post(
        self, suffix: str = "", json: Optional[Any] = None, params: Optional[Dict[str, Any]] = None
    ) -> Response:
        return self.session.post(f"{self.prefix}{suffix}", json=json, params=params)


class DatabricksApi(ABC):
    def __init__(self, session: Session, host: str, api: str):
        self.session = PrefixSession(session, host, api)


class PollableApi(DatabricksApi, ABC):
    def __init__(self, session: Session, host: str, api: str, polling_interval: int, timeout: int):
        super().__init__(session, host, api)
        self.timeout = timeout
        self.polling_interval = polling_interval

    def _poll_api(
        self,
        url: str,
        params: dict,
        get_state_func: Callable[[Response], str],
        terminal_states: Set[str],
        expected_end_state: str,
        unexpected_end_state_func: Callable[[Response], None],
    ) -> Response:
        state = None
        start = time.time()
        exceeded_timeout = False
        while state not in terminal_states:
            if time.time() - start > self.timeout:
                exceeded_timeout = True
                break
            # should we do exponential backoff?
            time.sleep(self.polling_interval)
            response = self.session.get(url, params=params)
            if response.status_code != 200:
                raise DbtRuntimeError(f"Error polling for completion.\n {response.content!r}")
            state = get_state_func(response)
        if exceeded_timeout:
            raise DbtRuntimeError("Python model run timed out")
        if state != expected_end_state:
            unexpected_end_state_func(response)

        return response


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommandExecution(object):
    command_id: str
    context_id: str
    cluster_id: str

    def model_dump(self) -> Dict[str, Any]:
        return {
            "commandId": self.command_id,
            "contextId": self.context_id,
            "clusterId": self.cluster_id,
        }


class CommandApi(PollableApi):
    def __init__(self, session: Session, host: str, polling_interval: int, timeout: int):
        super().__init__(session, host, "/api/1.2/commands", polling_interval, timeout)

    def execute(self, cluster_id: str, context_id: str, command: str) -> CommandExecution:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        response = self.session.post(
            "/execute",
            json={
                "clusterId": cluster_id,
                "contextId": context_id,
                "language": SUBMISSION_LANGUAGE,
                "command": command,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating a command.\n {response.content!r}")

        response_json = response.json()
        logger.debug(f"Command execution response={response_json}")
        return CommandExecution(
            command_id=response_json["id"], cluster_id=cluster_id, context_id=context_id
        )

    def cancel(self, command: CommandExecution) -> None:
        logger.debug(f"Cancelling command {command}")
        response = self.session.post("/cancel", json=command.model_dump())

        if response.status_code != 200:
            raise DbtRuntimeError(f"Cancel command {command} failed.\n {response.content!r}")

    def poll_for_completion(self, command: CommandExecution) -> None:
        self._poll_api(
            url="/status",
            params={
                "clusterId": command.cluster_id,
                "contextId": command.context_id,
                "commandId": command.command_id,
            },
            get_state_func=lambda response: response.json()["status"],
            terminal_states={"Finished", "Error", "Cancelled"},
            expected_end_state="Finished",
            unexpected_end_state_func=self._get_exception,
        )

    def _get_exception(self, response: Response) -> None:
        response_json = response.json()
        state = response_json["status"]
        state_message = response_json["results"]["data"]
        raise DbtRuntimeError(
            f"Python model run ended in state {state} with state_message\n{state_message}"
        )


class JobRunsApi(PollableApi):
    def __init__(self, session: Session, host: str, polling_interval: int, timeout: int):
        super().__init__(session, host, "/api/2.1/jobs/runs", polling_interval, timeout)

    def submit(self, run_name: str, job_spec: Dict[str, Any]) -> str:
        submit_response = self.session.post(
            "/submit", json={"run_name": run_name, "tasks": [job_spec]}
        )
        if submit_response.status_code != 200:
            raise DbtRuntimeError(f"Error creating python run.\n {submit_response.content!r}")

        logger.info(f"Job submission response={submit_response.content!r}")
        return submit_response.json()["run_id"]

    def poll_for_completion(self, run_id: str) -> None:
        self._poll_api(
            url="/get",
            params={"run_id": run_id},
            get_state_func=lambda response: response.json()["state"]["life_cycle_state"],
            terminal_states={"TERMINATED", "SKIPPED", "INTERNAL_ERROR"},
            expected_end_state="TERMINATED",
            unexpected_end_state_func=self._get_exception,
        )

    def _get_exception(self, response: Response) -> None:
        response_json = response.json()
        result_state = response_json["state"]["life_cycle_state"]
        if result_state != "SUCCESS":
            try:
                task_id = response_json["tasks"][0]["run_id"]
                # get end state to return to user
                run_output = self.session.get("/get-output", params={"run_id": task_id})
                json_run_output = run_output.json()
                raise DbtRuntimeError(
                    "Python model failed with traceback as:\n"
                    "(Note that the line number here does not "
                    "match the line number in your code due to dbt templating)\n"
                    f"{json_run_output['error']}\n"
                    f"{utils.remove_ansi(json_run_output.get('error_trace', ''))}"
                )

            except Exception as e:
                if isinstance(e, DbtRuntimeError):
                    raise e
                else:
                    state_message = response.json()["state"]["state_message"]
                    raise DbtRuntimeError(
                        f"Python model run ended in state {result_state} "
                        f"with state_message\n{state_message}"
                    )

    def cancel(self, run_id: str) -> None:
        logger.debug(f"Cancelling run id {run_id}")
        response = self.session.post("/cancel", json={"run_id": run_id})

        if response.status_code != 200:
            raise DbtRuntimeError(f"Cancel run {run_id} failed.\n {response.content!r}")


class DatabricksApiClient:
    def __init__(
        self,
        session: Session,
        workspace_client: WorkspaceClient,
        host: str,
        polling_interval: int,
        timeout: int,
        use_user_folder: bool,
    ):
        self.clusters = ClusterApi(workspace_client)
        self.command_contexts = CommandContextApi(workspace_client, self.clusters)
        if use_user_folder:
            self.folders: FolderApi = UserFolderApi(workspace_client)
        else:
            self.folders = SharedFolderApi()
        self.workspace = WorkspaceApi(workspace_client, self.folders)
        self.commands = CommandApi(session, host, polling_interval, timeout)
        self.job_runs = JobRunsApi(session, host, polling_interval, timeout)

    @staticmethod
    def create(
        credentials: DatabricksCredentials,
        timeout: int,
        use_user_folder: bool = False,
    ) -> "DatabricksApiClient":
        polling_interval = DEFAULT_POLLING_INTERVAL
        retry_strategy = Retry(total=4, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = Session()
        session.mount("https://", adapter)

        invocation_env = credentials.get_invocation_env()
        user_agent = USER_AGENT
        if invocation_env:
            user_agent = f"{user_agent} ({invocation_env})"

        connection_parameters = credentials.connection_parameters.copy()  # type: ignore[union-attr]

        http_headers = credentials.get_all_http_headers(
            connection_parameters.pop("http_headers", {})
        )
        credentials_provider = credentials.authenticate()
        header_factory = credentials_provider.credentials_provider()  # type: ignore
        session.auth = BearerAuth(header_factory)

        session.headers.update({"User-Agent": user_agent, **http_headers})
        host = credentials.host
        workspace_client = credentials.authenticate().api_client

        assert host is not None, "Host must be set in the credentials"
        return DatabricksApiClient(
            session, workspace_client, host, polling_interval, timeout, use_user_folder
        )
