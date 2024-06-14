import base64
import time
from calendar import c
from math import log
from nis import cat
from typing import Any
from typing import Callable
from typing import Dict
from typing import Set

from click import Command
from dbt.adapters.databricks import utils
from dbt.adapters.databricks.auth import BearerAuth
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.python_models.run_tracking import CommandExecution
from dbt_common.exceptions import DbtRuntimeError
from requests import Response
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"


class PythonApiClient:
    def __init__(self, credentials: DatabricksCredentials, version: str, timeout: int):
        self.polling_interval = DEFAULT_POLLING_INTERVAL
        self.timeout = timeout
        retry_strategy = Retry(total=4, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = Session()
        self.session.mount("https://", adapter)
        user_agent = f"dbt-databricks/{version}"

        invocation_env = credentials.get_invocation_env()
        if invocation_env:
            user_agent = f"{user_agent} ({invocation_env})"

        connection_parameters = credentials.connection_parameters.copy()  # type: ignore[union-attr]

        http_headers = credentials.get_all_http_headers(
            connection_parameters.pop("http_headers", {})
        )
        self._credentials_provider = credentials.authenticate(None)
        header_factory = self._credentials_provider(None)  # type: ignore
        self.session.auth = BearerAuth(header_factory)

        self.session.headers.update({"User-Agent": user_agent, **http_headers})
        self.host = credentials.host
        self.user_folder = ""

    def create_workdir(self, catalog: str, schema: str) -> str:
        user_folder = self._get_user_folder()

        path = f"{user_folder}/dbt_python_models/{catalog}/{schema}/"
        response = self.session.post(
            f"https://{self.host}/api/2.0/workspace/mkdirs",
            json={
                "path": path,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error creating work_dir for python notebooks\n {response.content!r}"
            )

        return path

    def _get_user_folder(self) -> str:
        if not self.user_folder:
            try:
                response = self.session.post(f"https://{self.host}/api/2.0/preview/scim/v2/Me")

                if response.status_code != 200:
                    raise DbtRuntimeError(
                        f"Error getting user folder for python notebooks\n {response.content!r}"
                    )
                user = response.json()["userName"]
                self.user_folder = f"/Users/{user}"
            except Exception:
                logger.warning("Error getting user folder for python notebooks, using /Shared")
                self.user_folder = "/Shared"
        return self.user_folder

    def upload_notebook(self, path: str, compiled_code: str) -> None:
        b64_encoded_content = base64.b64encode(compiled_code.encode()).decode()
        response = self.session.post(
            f"https://{self.host}/api/2.0/workspace/import",
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

    def submit_job(self, run_name: str, job_spec: Dict[str, Any]) -> str:
        submit_response = self.session.post(
            f"https://{self.host}/api/2.1/jobs/runs/submit",
            json={
                "run_name": run_name,
                "tasks": [job_spec],
            },
        )
        if submit_response.status_code != 200:
            raise DbtRuntimeError(f"Error creating python run.\n {submit_response.content!r}")

        logger.info(f"Job submission response={submit_response}")
        return submit_response.json()["run_id"]

    def poll_for_job_completion(self, run_id: str) -> None:
        self._poll_api(
            url=f"https://{self.host}/api/2.1/jobs/runs/get",
            params={"run_id": run_id},
            get_state_func=lambda response: response.json()["state"]["life_cycle_state"],
            terminal_states={"TERMINATED", "SKIPPED", "INTERNAL_ERROR"},
            expected_end_state="TERMINATED",
            unexpected_end_state_func=self._get_task_exception,
        )

    def _get_task_exception(self, response: Response) -> None:
        result_state = response.json()["state"]["result_state"]
        if result_state != "SUCCESS":
            try:
                task_id = response.json()["tasks"][0]["run_id"]
                # get end state to return to user
                run_output = self.session.get(
                    f"https://{self.host}/api/2.1/jobs/runs/get-output",
                    params={"run_id": task_id},
                )
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
            state = get_state_func(response)
        if exceeded_timeout:
            raise DbtRuntimeError("Python model run timed out")
        if state != expected_end_state:
            unexpected_end_state_func(response)

        return response

    def create_command_context(self, cluster_id: str) -> str:
        current_status = self.get_cluster_status(cluster_id)

        if current_status in ["TERMINATED", "TERMINATING"]:
            logger.debug(f"Cluster {cluster_id} is not running. Attempting to restart.")
            self.start_cluster(cluster_id)
            logger.debug(f"Cluster {cluster_id} is now running.")
        elif current_status != "RUNNING":
            self.wait_for_cluster_to_start(cluster_id)

        response = self.session.post(
            f"https://{self.host}/api/1.2/contexts/create",
            json={
                "clusterId": cluster_id,
                "language": SUBMISSION_LANGUAGE,
            },
        )
        logger.info(f"Creating execution context response={response}")

        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating an execution context.\n {response.content!r}")
        return response.json()["id"]

    def get_cluster_status(self, cluster_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#get

        response = self.session.get(
            f"https://{self.host}/api/2.0/clusters/get",
            json={"cluster_id": cluster_id},
        )
        logger.debug(f"Cluster status response={response}")
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error getting status of cluster.\n {response.content!r}")

        json_response = response.json()
        return json_response.get("state", "").upper()

    def start_cluster(self, cluster_id: str) -> None:
        """Send the start command and poll for the cluster status until it shows "Running"

        Raise an exception if the restart exceeds our timeout.
        """

        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#start

        response = self.session.post(
            f"https://{self.host}/api/2.0/clusters/start",
            json={"cluster_id": cluster_id},
        )
        logger.debug(f"Cluster start response={response}")
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error starting terminated cluster.\n {response.content!r}")

    def wait_for_cluster_to_start(self, cluster_id: str) -> None:
        max_cluster_start_time = 900
        start_time = time.time()

        while time.time() - start_time < max_cluster_start_time:
            status_response = self.get_cluster_status(cluster_id)
            if status_response == "RUNNING":
                return
            else:
                time.sleep(5)

        raise DbtRuntimeError(
            f"Cluster {cluster_id} restart timed out after {max_cluster_start_time} seconds"
        )

    def destroy_command_context(self, cluster_id: str, context_id: str) -> str:
        response = self.session.post(
            f"https://{self.host}/api/1.2/contexts/destroy",
            json={
                "clusterId": cluster_id,
                "contextId": context_id,
            },
        )
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error deleting an execution context.\n {response.content!r}")
        return response.json()["id"]

    def execute_command(self, cluster_id: str, context_id: str, command: str) -> CommandExecution:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        response = self.session.post(
            f"https://{self.host}/api/1.2/commands/execute",
            json={
                "clusterId": cluster_id,
                "contextId": context_id,
                "language": SUBMISSION_LANGUAGE,
                "command": command,
            },
        )
        response_json = response.json()
        logger.debug(f"Command execution response={response_json}")
        if response.status_code != 200:
            raise DbtRuntimeError(f"Error creating a command.\n {response.content!r}")
        return CommandExecution(
            command_id=response_json["id"], cluster_id=cluster_id, context_id=context_id
        )

    def poll_for_command_completion(self, command: CommandExecution) -> None:
        self._poll_api(
            url=f"https://{self.host}/api/1.2/commands/status",
            params={
                "clusterId": command.cluster_id,
                "contextId": command.context_id,
                "commandId": command.command_id,
            },
            get_state_func=lambda response: response.json()["status"],
            terminal_states={"Finished", "Error", "Cancelled"},
            expected_end_state="Finished",
            unexpected_end_state_func=self._get_command_exception,
        )

    def _get_command_exception(self, response: Response) -> None:
        response_json = response.json()
        state = response_json["status"]
        state_message = response_json["results"]["data"]
        raise DbtRuntimeError(
            f"Python model run ended in state {state} " f"with state_message\n{state_message}"
        )
