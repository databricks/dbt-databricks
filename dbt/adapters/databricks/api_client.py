import base64
import json
import re
import time
import urllib.parse
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional

from dbt_common.exceptions import DbtRuntimeError
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dbt.adapters.databricks import utils
from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.credentials import BearerAuth, DatabricksCredentials
from dbt.adapters.databricks.logging import logger

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
USER_AGENT = f"dbt-databricks/{version}"
LIBRARY_VALID_STATUSES = {"INSTALLED", "RESTORED", "SKIPPED"}


class PrefixSession:
    def __init__(self, session: Session, host: str, api: str):
        self.prefix = f"https://{host}{api}"
        self.session = session

    def get(
        self, suffix: str = "", json: Optional[Any] = None, params: Optional[dict[str, Any]] = None
    ) -> Response:
        return self.session.get(f"{self.prefix}{suffix}", json=json, params=params)

    def post(
        self, suffix: str = "", json: Optional[Any] = None, params: Optional[dict[str, Any]] = None
    ) -> Response:
        return self.session.post(f"{self.prefix}{suffix}", json=json, params=params)

    def put(
        self, suffix: str = "", json: Optional[Any] = None, params: Optional[dict[str, Any]] = None
    ) -> Response:
        return self.session.put(f"{self.prefix}{suffix}", json=json, params=params)

    def patch(
        self,
        suffix: str = "",
        payload: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> Response:
        return self.session.patch(f"{self.prefix}{suffix}", data=json.dumps(payload), params=params)


class DatabricksApi(ABC):
    def __init__(self, session: Session, host: str, api: str):
        self.session = PrefixSession(session, host, api)


class LibraryApi(DatabricksApi):
    def __init__(self, session: Session, host: str):
        super().__init__(session, host, "/api/2.0/libraries")

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
        response = self.session.get(
            "/cluster-status",
            json={"cluster_id": cluster_id},
        )
        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error getting status of libraries of a cluster.\n {response.content!r}"
            )

        json_response = response.json()
        return json_response


class ClusterApi(DatabricksApi):
    def __init__(
        self,
        session: Session,
        host: str,
        libraries: LibraryApi,
        max_cluster_start_time: int = 900,
    ):
        super().__init__(session, host, "/api/2.0/clusters")
        self.max_cluster_start_time = max_cluster_start_time
        self.libraries = libraries

    def status(self, cluster_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#get

        response = self.session.get("/get", json={"cluster_id": cluster_id})
        logger.debug(f"Cluster status response={response.content!r}")
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error getting status of cluster.\n {response.content!r}")

        json_response = response.json()
        return json_response.get("state", "").upper()

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

        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#start

        response = self.session.post("/start", json={"cluster_id": cluster_id})
        if response.status_code != 200:
            if self.status(cluster_id) not in ["RUNNING", "PENDING"]:
                raise DbtRuntimeError(f"Error starting terminated cluster.\n {response.content!r}")
            else:
                logger.debug("Presuming race condition, waiting for cluster to start")

        self.wait_for_cluster(cluster_id)


class CommandContextApi(DatabricksApi):
    def __init__(
        self, session: Session, host: str, cluster_api: ClusterApi, library_api: LibraryApi
    ):
        super().__init__(session, host, "/api/1.2/contexts")
        self.cluster_api = cluster_api
        self.library_api = library_api

    def create(self, cluster_id: str) -> str:
        current_status = self.cluster_api.status(cluster_id)

        if current_status in ["TERMINATED", "TERMINATING"]:
            logger.debug(f"Cluster {cluster_id} is not running. Attempting to restart.")
            self.cluster_api.start(cluster_id)
            logger.debug(f"Cluster {cluster_id} is now running.")
        elif current_status != "RUNNING" or not self.library_api.all_libraries_installed(
            cluster_id
        ):
            self.cluster_api.wait_for_cluster(cluster_id)

        response = self.session.post(
            "/create", json={"clusterId": cluster_id, "language": SUBMISSION_LANGUAGE}
        )
        logger.info(f"Creating execution context response={response}")

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating an execution context.\n {response.content!r}")
        return response.json()["id"]

    def destroy(self, cluster_id: str, context_id: str) -> None:
        response = self.session.post(
            "/destroy", json={"clusterId": cluster_id, "contextId": context_id}
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error deleting an execution context.\n {response.content!r}")


class FolderApi(ABC):
    @abstractmethod
    def get_folder(self, catalog: str, schema: str) -> str:
        pass


# Use this for now to not break users
class SharedFolderApi(FolderApi):
    def get_folder(self, _: str, schema: str) -> str:
        return f"/Shared/dbt_python_models/{schema}/"


class CurrUserApi(DatabricksApi):
    def __init__(self, session: Session, host: str):
        super().__init__(session, host, "/api/2.0/preview/scim/v2")
        self._user = ""

    def get_username(self) -> str:
        if self._user:
            return self._user

        response = self.session.get("/Me")
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error getting current user.\n {response.content!r}")

        username = response.json()["userName"]
        self._user = username
        return username

    def is_service_principal(self, username: str) -> bool:
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(uuid_pattern, username, re.IGNORECASE))


# Switch to this as part of 2.0.0 release
class UserFolderApi(DatabricksApi, FolderApi):
    def __init__(self, session: Session, host: str, user_api: CurrUserApi):
        super().__init__(session, host, "/api/2.0/preview/scim/v2")
        self.user_api = user_api

    def get_folder(self, catalog: str, schema: str) -> str:
        username = self.user_api.get_username()
        folder = f"/Users/{username}/dbt_python_models/{catalog}/{schema}/"
        logger.debug(f"Using python model folder '{folder}'")

        return folder


class WorkspaceApi(DatabricksApi):
    def __init__(self, session: Session, host: str, folder_api: FolderApi):
        super().__init__(session, host, "/api/2.0/workspace")
        self.user_api = folder_api

    def create_python_model_dir(self, catalog: str, schema: str) -> str:
        folder = self.user_api.get_folder(catalog, schema)

        response = self.session.post("/mkdirs", json={"path": folder})
        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error creating work_dir for python notebooks\n {response.content!r}"
            )

        return folder

    def upload_notebook(self, path: str, compiled_code: str) -> None:
        b64_encoded_content = base64.b64encode(compiled_code.encode()).decode()
        response = self.session.post(
            "/import",
            json={
                "path": path,
                "content": b64_encoded_content,
                "language": "PYTHON",
                "overwrite": True,
                "format": "SOURCE",
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating python notebook.\n {response.content!r}")

    def get_object_id(self, path: str) -> int:
        """
        Get the workspace object ID for the specified path
        """
        response = self.session.get("/get-status", params={"path": path})

        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error getting workspace object ID for {path}.\n {response.content!r}"
            )

        try:
            object_info = response.json()
            object_id = object_info.get("object_id")

            if not object_id:
                raise DbtRuntimeError(f"No object_id found for path {path}")

            return object_id
        except Exception as e:
            raise DbtRuntimeError(f"Error parsing workspace object info: {str(e)}")


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
        terminal_states: set[str],
        expected_end_state: Optional[str],
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
        response = self._poll_api(
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
        ).json()

        if response["results"]["resultType"] == "error":
            raise DbtRuntimeError(
                f"Python model failed with traceback as:\n"
                f"{utils.remove_ansi(response['results']['cause'])}"
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

    def submit(
        self, run_name: str, job_spec: dict[str, Any], **additional_job_settings: dict[str, Any]
    ) -> str:
        logger.debug(
            f"Submitting job with run_name={run_name} and job_spec={job_spec}"
            " and additional_job_settings={additional_job_settings}"
        )
        submit_response = self.session.post(
            "/submit", json={"run_name": run_name, "tasks": [job_spec], **additional_job_settings}
        )
        if submit_response.status_code != 200:
            raise DbtRuntimeError(f"Error creating python run.\n {submit_response.content!r}")

        logger.info(f"Job submission response={submit_response.content!r}")
        return submit_response.json()["run_id"]

    def get_run_info(self, run_id: str) -> dict[str, Any]:
        response = self.session.get("/get", params={"run_id": run_id})

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error getting run info.\n {response.content!r}")

        return response.json()

    def get_job_id_from_run_id(self, run_id: str) -> str:
        run_info = self.get_run_info(run_id)
        job_id = run_info.get("job_id")

        if not job_id:
            raise DbtRuntimeError(f"Could not get job_id from run_id {run_id}")

        return str(job_id)

    def poll_for_completion(self, run_id: str) -> None:
        self._poll_api(
            url="/get",
            params={"run_id": run_id},
            get_state_func=lambda response: response.json()["state"]["life_cycle_state"],
            terminal_states={"TERMINATED", "SKIPPED", "INTERNAL_ERROR"},
            expected_end_state=None,
            unexpected_end_state_func=self._get_exception,
        )

    def _get_exception(self, response: Response) -> None:
        response_json = response.json()
        state = response_json["state"]
        result_state = state.get("result_state")
        life_cycle_state = state["life_cycle_state"]

        if result_state == "CANCELED":
            raise DbtRuntimeError(f"Python model run ended in result_state {result_state}")

        if life_cycle_state != "TERMINATED":
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
                        f"Python model run ended in state {life_cycle_state}"
                        f"with state_message\n{state_message}"
                    )

    def cancel(self, run_id: str) -> None:
        logger.debug(f"Cancelling run id {run_id}")
        response = self.session.post("/cancel", json={"run_id": run_id})

        if response.status_code != 200:
            raise DbtRuntimeError(f"Cancel run {run_id} failed.\n {response.content!r}")


class JobPermissionsApi(DatabricksApi):
    def __init__(self, session: Session, host: str):
        super().__init__(session, host, "/api/2.0/permissions/jobs")

    def put(self, job_id: str, access_control_list: list[dict[str, Any]]) -> None:
        request_body = {"access_control_list": access_control_list}

        response = self.session.put(f"/{job_id}", json=request_body)
        logger.debug(f"Workflow permissions update response={response.json()}")

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error updating Databricks workflow.\n {response.content!r}")

    def patch(self, job_id: str, access_control_list: list[dict[str, Any]]) -> None:
        request_body = {"access_control_list": access_control_list}

        response = self.session.patch(f"/{job_id}", payload=request_body)
        logger.debug(f"Workflow permissions update response={response.json()}")

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error updating Databricks workflow.\n {response.content!r}")

    def get(self, job_id: str) -> dict[str, Any]:
        response = self.session.get(f"/{job_id}")

        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error fetching Databricks workflow permissions.\n {response.content!r}"
            )

        return response.json()


class NotebookPermissionsApi(DatabricksApi):
    def __init__(self, session: Session, host: str, workspace_api: "WorkspaceApi"):
        super().__init__(session, host, "/api/2.0/permissions/notebooks")
        self.workspace_api = workspace_api

    def put(self, notebook_path: str, access_control_list: list[dict[str, Any]]) -> None:
        request_body = {"access_control_list": access_control_list}

        object_id = self.workspace_api.get_object_id(notebook_path)
        encoded_id = urllib.parse.quote(str(object_id))

        logger.debug(
            f"Setting notebook permissions for path={notebook_path}, object_id={object_id}"
        )
        logger.debug(f"Permission request body: {request_body}")

        response = self.session.put(f"/{encoded_id}", json=request_body)

        if response.status_code != 200:
            error_msg = (
                f"Error updating Databricks notebook permissions.\n"
                f"path={notebook_path}, object_id={object_id}\n"
                f"request_body={request_body}\n"
                f"response={response.content!r}"
            )
            logger.error(error_msg)
            raise DbtRuntimeError(error_msg)

    def get(self, notebook_path: str) -> dict[str, Any]:
        object_id = self.workspace_api.get_object_id(notebook_path)
        encoded_id = urllib.parse.quote(str(object_id))

        logger.debug(
            f"Getting notebook permissions for path={notebook_path}, object_id={object_id}"
        )

        response = self.session.get(f"/{encoded_id}")

        if response.status_code != 200:
            error_msg = (
                f"Error fetching Databricks notebook permissions.\n"
                f"path={notebook_path}, object_id={object_id}\n"
                f"response={response.content!r}"
            )
            logger.error(error_msg)
            raise DbtRuntimeError(error_msg)

        return response.json()


class WorkflowJobApi(DatabricksApi):
    def __init__(self, session: Session, host: str):
        super().__init__(session, host, "/api/2.1/jobs")

    def search_by_name(self, job_name: str) -> list[dict[str, Any]]:
        response = self.session.get("/list", json={"name": job_name})

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error fetching job by name.\n {response.content!r}")

        return response.json().get("jobs", [])

    def create(self, job_spec: dict[str, Any]) -> str:
        """
        :return: the job_id
        """
        response = self.session.post("/create", json=job_spec)

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating Workflow.\n {response.content!r}")

        job_id = response.json()["job_id"]
        logger.info(f"New workflow created with job id {job_id}")
        return job_id

    def update_job_settings(self, job_id: str, job_spec: dict[str, Any]) -> None:
        request_body = {
            "job_id": job_id,
            "new_settings": job_spec,
        }
        logger.debug(f"Job settings: {request_body}")
        response = self.session.post("/reset", json=request_body)

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error updating Workflow.\n {response.content!r}")

        logger.debug(f"Workflow update response={response.json()}")

    def run(self, job_id: str, enable_queueing: bool = True) -> str:
        request_body = {
            "job_id": job_id,
            "queue": {
                "enabled": enable_queueing,
            },
        }
        response = self.session.post("/run-now", json=request_body)

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error triggering run for workflow.\n {response.content!r}")

        response_json = response.json()
        logger.info(f"Workflow trigger response={response_json}")

        return response_json["run_id"]


class DltPipelineApi(PollableApi):
    def __init__(self, session: Session, host: str, polling_interval: int):
        super().__init__(session, host, "/api/2.0/pipelines", polling_interval, 60 * 60)

    def poll_for_completion(self, pipeline_id: str) -> None:
        self._poll_api(
            url=f"/{pipeline_id}",
            params={},
            get_state_func=lambda response: response.json()["state"],
            terminal_states={"IDLE", "FAILED", "DELETED"},
            expected_end_state="IDLE",
            unexpected_end_state_func=self._get_exception,
        )

    def _get_exception(self, response: Response) -> None:
        response_json = response.json()
        cause = response_json.get("cause")
        if cause:
            raise DbtRuntimeError(f"Pipeline {response_json.get('pipeline_id')} failed: {cause}")
        else:
            latest_update = response_json.get("latest_updates")[0]
            last_error = self.get_update_error(response_json.get("pipeline_id"), latest_update)
            raise DbtRuntimeError(
                f"Pipeline {response_json.get('pipeline_id')} failed: {last_error}"
            )

    def get_update_error(self, pipeline_id: str, update_id: str) -> str:
        response = self.session.get(f"/{pipeline_id}/events")
        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error getting pipeline event info for {pipeline_id}: {response.text}"
            )

        events = response.json().get("events", [])
        update_events = [
            e
            for e in events
            if e.get("event_type", "") == "update_progress"
            and e.get("origin", {}).get("update_id") == update_id
        ]

        error_events = [
            e
            for e in update_events
            if e.get("details", {}).get("update_progress", {}).get("state", "") == "FAILED"
        ]

        msg = ""
        if error_events:
            msg = error_events[0].get("message", "")

        return msg


class DatabricksApiClient:
    def __init__(
        self,
        session: Session,
        host: str,
        polling_interval: int,
        timeout: int,
        use_user_folder: bool,
    ):
        self.libraries = LibraryApi(session, host)
        self.clusters = ClusterApi(session, host, self.libraries)
        self.command_contexts = CommandContextApi(session, host, self.clusters, self.libraries)
        self.curr_user = CurrUserApi(session, host)
        if use_user_folder:
            self.folders: FolderApi = UserFolderApi(session, host, self.curr_user)
        else:
            self.folders = SharedFolderApi()
        self.workspace = WorkspaceApi(session, host, self.folders)
        self.commands = CommandApi(session, host, polling_interval, timeout)
        self.job_runs = JobRunsApi(session, host, polling_interval, timeout)
        self.workflows = WorkflowJobApi(session, host)
        self.workflow_permissions = JobPermissionsApi(session, host)
        self.notebook_permissions = NotebookPermissionsApi(session, host, self.workspace)
        self.dlt_pipelines = DltPipelineApi(session, host, polling_interval)

    @staticmethod
    def create(
        credentials: DatabricksCredentials, timeout: int, use_user_folder: bool = False
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
        header_factory = credentials.authenticate().credentials_provider()
        session.auth = BearerAuth(header_factory)

        session.headers.update({"User-Agent": user_agent, **http_headers})
        host = credentials.host

        assert host is not None, "Host must be set in the credentials"
        return DatabricksApiClient(session, host, polling_interval, timeout, use_user_folder)
