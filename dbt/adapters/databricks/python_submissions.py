from typing import Any, Dict, Tuple, Optional, Callable

from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.connections import DatabricksCredentials

import base64
import time
import requests
import uuid

from dbt.events import AdapterLogger
import dbt.exceptions
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.spark import __version__
from databricks.sdk.core import CredentialsProvider

logger = AdapterLogger("Databricks")

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
DEFAULT_TIMEOUT = 60 * 60 * 24
DBT_SPARK_VERSION = __version__.version


class BaseDatabricksHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: DatabricksCredentials) -> None:
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.parsed_model = parsed_model
        self.timeout = self.get_timeout()
        self.polling_interval = DEFAULT_POLLING_INTERVAL
        self.check_credentials()
        self.auth_header = {
            "Authorization": f"Bearer {self.credentials.token}",
            "User-Agent": f"dbt-labs-dbt-spark/{DBT_SPARK_VERSION} (Databricks)",
        }

    @property
    def cluster_id(self) -> str:
        return self.parsed_model["config"].get("cluster_id", self.credentials.cluster_id)

    def get_timeout(self) -> int:
        timeout = self.parsed_model["config"].get("timeout", DEFAULT_TIMEOUT)
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        return timeout

    def check_credentials(self) -> None:
        raise NotImplementedError(
            "Overwrite this method to check specific requirement for current submission method"
        )

    def _create_work_dir(self, path: str) -> None:
        response = requests.post(
            f"https://{self.credentials.host}/api/2.0/workspace/mkdirs",
            headers=self.auth_header,
            json={
                "path": path,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error creating work_dir for python notebooks\n {response.content!r}"
            )

    def _upload_notebook(self, path: str, compiled_code: str) -> None:
        b64_encoded_content = base64.b64encode(compiled_code.encode()).decode()
        response = requests.post(
            f"https://{self.credentials.host}/api/2.0/workspace/import",
            headers=self.auth_header,
            json={
                "path": path,
                "content": b64_encoded_content,
                "language": "PYTHON",
                "overwrite": True,
                "format": "SOURCE",
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error creating python notebook.\n {response.content!r}"
            )

    def _submit_job(self, path: str, cluster_spec: dict) -> str:
        job_spec = {
            "run_name": f"{self.schema}-{self.identifier}-{uuid.uuid4()}",
            "notebook_task": {
                "notebook_path": path,
            },
        }
        job_spec.update(cluster_spec)  # updates 'new_cluster' config
        # PYPI packages
        packages = self.parsed_model["config"].get("packages", [])
        # additional format of packages
        additional_libs = self.parsed_model["config"].get("additional_libs", [])
        libraries = []
        for package in packages:
            libraries.append({"pypi": {"package": package}})
        for lib in additional_libs:
            libraries.append(lib)
        job_spec.update({"libraries": libraries})  # type: ignore
        submit_response = requests.post(
            f"https://{self.credentials.host}/api/2.1/jobs/runs/submit",
            headers=self.auth_header,
            json=job_spec,
        )
        if submit_response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error creating python run.\n {submit_response.content!r}"
            )
        return submit_response.json()["run_id"]

    def _submit_through_notebook(self, compiled_code: str, cluster_spec: dict) -> None:
        # it is safe to call mkdirs even if dir already exists and have content inside
        work_dir = f"/Shared/dbt_python_model/{self.schema}/"
        self._create_work_dir(work_dir)
        # add notebook
        whole_file_path = f"{work_dir}{self.identifier}"
        self._upload_notebook(whole_file_path, compiled_code)

        # submit job
        run_id = self._submit_job(whole_file_path, cluster_spec)

        self.polling(
            status_func=requests.get,
            status_func_kwargs={
                "url": f"https://{self.credentials.host}/api/2.1/jobs/runs/get?run_id={run_id}",
                "headers": self.auth_header,
            },
            get_state_func=lambda response: response.json()["state"]["life_cycle_state"],
            terminal_states=("TERMINATED", "SKIPPED", "INTERNAL_ERROR"),
            expected_end_state="TERMINATED",
            get_state_msg_func=lambda response: response.json()["state"]["state_message"],
        )

        # get end state to return to user
        run_output = requests.get(
            f"https://{self.credentials.host}" f"/api/2.1/jobs/runs/get-output?run_id={run_id}",
            headers=self.auth_header,
        )
        json_run_output = run_output.json()
        result_state = json_run_output["metadata"]["state"]["result_state"]
        if result_state != "SUCCESS":
            raise dbt.exceptions.DbtRuntimeError(
                "Python model failed with traceback as:\n"
                "(Note that the line number here does not "
                "match the line number in your code due to dbt templating)\n"
                f"{json_run_output['error_trace']}"
            )

    def submit(self, compiled_code: str) -> None:
        raise NotImplementedError(
            "BasePythonJobHelper is an abstract class and you should implement submit method."
        )

    def polling(
        self,
        status_func: Callable,
        status_func_kwargs: Dict,
        get_state_func: Callable,
        terminal_states: Tuple[str, ...],
        expected_end_state: str,
        get_state_msg_func: Callable,
    ) -> Dict:
        state = None
        start = time.time()
        exceeded_timeout = False
        response = {}
        while state not in terminal_states:
            if time.time() - start > self.timeout:
                exceeded_timeout = True
                break
            # should we do exponential backoff?
            time.sleep(self.polling_interval)
            response = status_func(**status_func_kwargs)
            state = get_state_func(response)
        if exceeded_timeout:
            raise dbt.exceptions.DbtRuntimeError("python model run timed out")
        if state != expected_end_state:
            raise dbt.exceptions.DbtRuntimeError(
                "python model run ended in state"
                f"{state} with state_message\n{get_state_msg_func(response)}"
            )
        return response


class JobClusterPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self) -> None:
        if not self.parsed_model["config"].get("job_cluster_config", None):
            raise ValueError("job_cluster_config is required for commands submission method.")

    def submit(self, compiled_code: str) -> None:
        cluster_spec = {"new_cluster": self.parsed_model["config"]["job_cluster_config"]}
        self._submit_through_notebook(compiled_code, cluster_spec)


class DBContext:
    def __init__(
        self, credentials: DatabricksCredentials, cluster_id: str, auth_header: dict
    ) -> None:
        self.auth_header = auth_header
        self.cluster_id = cluster_id
        self.host = credentials.host

    def create(self) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#create-an-execution-context

        current_status = self.get_cluster_status().get("state", "").upper()
        if current_status in ["TERMINATED", "TERMINATING"]:
            logger.debug(f"Cluster {self.cluster_id} is not running. Attempting to restart.")
            self.start_cluster()
            logger.debug(f"Cluster {self.cluster_id} is now running.")

        response = requests.post(
            f"https://{self.host}/api/1.2/contexts/create",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster_id,
                "language": SUBMISSION_LANGUAGE,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error creating an execution context.\n {response.content!r}"
            )
        return response.json()["id"]

    def destroy(self, context_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#delete-an-execution-context
        response = requests.post(
            f"https://{self.host}/api/1.2/contexts/destroy",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster_id,
                "contextId": context_id,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error deleting an execution context.\n {response.content!r}"
            )
        return response.json()["id"]

    def get_cluster_status(self) -> Dict:
        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#get

        response = requests.get(
            f"https://{self.host}/api/2.0/clusters/get",
            headers=self.auth_header,
            json={"cluster_id": self.cluster_id},
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error getting status of cluster.\n {response.content!r}"
            )

        json_response = response.json()
        return json_response

    def start_cluster(self) -> None:
        """Send the start command and poll for the cluster status until it shows "Running"

        Raise an exception if the restart exceeds our timeout.
        """

        # https://docs.databricks.com/dev-tools/api/latest/clusters.html#start

        logger.debug(f"Sending restart command for cluster id {self.cluster_id}")

        response = requests.post(
            f"https://{self.host}/api/2.0/clusters/start",
            headers=self.auth_header,
            json={"cluster_id": self.cluster_id},
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error starting terminated cluster.\n {response.content!r}"
            )

        # seconds
        MAX_CLUSTER_START_TIME = 900
        start_time = time.time()

        def get_elapsed() -> float:
            return time.time() - start_time

        while get_elapsed() < MAX_CLUSTER_START_TIME:
            status_response = self.get_cluster_status()
            if str(status_response.get("state")).lower() == "running":
                return
            else:
                time.sleep(5)

        raise dbt.exceptions.DbtRuntimeError(
            f"Cluster {self.cluster_id} restart timed out after {MAX_CLUSTER_START_TIME} seconds"
        )


class DBCommand:
    def __init__(
        self, credentials: DatabricksCredentials, cluster_id: str, auth_header: dict
    ) -> None:
        self.auth_header = auth_header
        self.cluster_id = cluster_id
        self.host = credentials.host

    def execute(self, context_id: str, command: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        response = requests.post(
            f"https://{self.host}/api/1.2/commands/execute",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster_id,
                "contextId": context_id,
                "language": SUBMISSION_LANGUAGE,
                "command": command,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error creating a command.\n {response.content!r}"
            )
        return response.json()["id"]

    def status(self, context_id: str, command_id: str) -> Dict[str, Any]:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#get-information-about-a-command
        response = requests.get(
            f"https://{self.host}/api/1.2/commands/status",
            headers=self.auth_header,
            params={
                "clusterId": self.cluster_id,
                "contextId": context_id,
                "commandId": command_id,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.DbtRuntimeError(
                f"Error getting status of command.\n {response.content!r}"
            )
        return response.json()


class AllPurposeClusterPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self) -> None:
        if not self.cluster_id:
            raise ValueError(
                "Databricks cluster_id is required for all_purpose_cluster submission method with\
                      running with notebook."
            )

    def submit(self, compiled_code: str) -> None:
        if self.parsed_model["config"].get("create_notebook", False):
            self._submit_through_notebook(compiled_code, {"existing_cluster_id": self.cluster_id})
        else:
            context = DBContext(self.credentials, self.cluster_id, self.auth_header)
            command = DBCommand(self.credentials, self.cluster_id, self.auth_header)
            context_id = context.create()
            try:
                command_id = command.execute(context_id, compiled_code)
                # poll until job finish
                response = self.polling(
                    status_func=command.status,
                    status_func_kwargs={
                        "context_id": context_id,
                        "command_id": command_id,
                    },
                    get_state_func=lambda response: response["status"],
                    terminal_states=("Cancelled", "Error", "Finished"),
                    expected_end_state="Finished",
                    get_state_msg_func=lambda response: response.json()["results"]["data"],
                )
                if response["results"]["resultType"] == "error":
                    raise dbt.exceptions.DbtRuntimeError(
                        f"Python model failed with traceback as:\n"
                        f"{response['results']['cause']}"
                    )
            finally:
                context.destroy(context_id)


class DbtDatabricksBasePythonJobHelper(BaseDatabricksHelper):
    credentials: DatabricksCredentials  # type: ignore[assignment]
    _credentials_provider: CredentialsProvider = None

    def __init__(self, parsed_model: Dict, credentials: DatabricksCredentials) -> None:
        super().__init__(
            parsed_model=parsed_model, credentials=credentials  # type: ignore[arg-type]
        )

        self.database = parsed_model.get("database")

        user_agent = f"dbt-databricks/{version}"

        invocation_env = credentials.get_invocation_env()
        if invocation_env:
            user_agent = f"{user_agent} ({invocation_env})"

        connection_parameters = credentials.connection_parameters.copy()  # type: ignore[union-attr]

        http_headers: Dict[str, str] = credentials.get_all_http_headers(
            connection_parameters.pop("http_headers", {})
        )
        self._credentials_provider = credentials.authenticate(self._credentials_provider)
        header_factory = self._credentials_provider()
        headers = header_factory()

        self.auth_header.update({"User-Agent": user_agent, **http_headers, **headers})

    @property
    def cluster_id(self) -> Optional[str]:  # type: ignore[override]
        return self.parsed_model["config"].get(
            "cluster_id",
            self.credentials.extract_cluster_id(
                self.parsed_model["config"].get("http_path", self.credentials.http_path)
            ),
        )

    def _work_dir(self, path: str) -> str:
        if self.database:
            return path.replace(f"/{self.schema}/", f"/{self.database}/{self.schema}/")
        else:
            return path

    def _create_work_dir(self, path: str) -> None:
        super()._create_work_dir(self._work_dir(path))

    def _upload_notebook(self, path: str, compiled_code: str) -> None:
        super()._upload_notebook(self._work_dir(path), compiled_code)

    def _submit_job(self, path: str, cluster_spec: dict) -> str:
        return super()._submit_job(self._work_dir(path), cluster_spec)


class DbtDatabricksJobClusterPythonJobHelper(
    DbtDatabricksBasePythonJobHelper, JobClusterPythonJobHelper
):
    def check_credentials(self) -> None:
        self.credentials.validate_creds()
        if not self.parsed_model["config"].get("job_cluster_config", None):
            raise ValueError(
                "`job_cluster_config` is required for the `job_cluster` submission method."
            )


class DbtDatabricksAllPurposeClusterPythonJobHelper(
    DbtDatabricksBasePythonJobHelper, AllPurposeClusterPythonJobHelper
):
    def check_credentials(self) -> None:
        self.credentials.validate_creds()
        if not self.cluster_id:
            raise ValueError(
                "Databricks `http_path` or `cluster_id` of an all-purpose cluster is required "
                "for the `all_purpose_cluster` submission method."
            )
