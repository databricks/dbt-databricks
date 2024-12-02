import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from typing import Any, Optional, Protocol, TypeVar

from dbt_common.exceptions import DbtRuntimeError
from requests import Response, Session
from requests.adapters import HTTPAdapter
from typing_extensions import Self
from urllib3.util.retry import Retry

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import CommandStatus, ContextStatus, Language
from databricks.sdk.service.iam import User
from databricks.sdk.service.jobs import (
    GitSource,
    JobAccessControlRequest,
    JobEmailNotifications,
    JobEnvironment,
    JobNotificationSettings,
    JobRunAs,
    JobsHealthRules,
    QueueSettings,
    Run,
    RunResultState,
    SubmitTask,
    TerminationTypeType,
    WebhookNotifications,
)
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.workspace import Language as WorkspaceLanguage
from dbt.adapters.databricks import utils
from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.credentials import (
    BearerAuth,
    DatabricksCredentialManager,
    DatabricksCredentials,
)
from dbt.adapters.databricks.logging import logger

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
USER_AGENT = f"dbt-databricks/{version}"


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


class DatabricksApi(ABC):
    def __init__(self, session: Session, host: str, api: str):
        self.session = PrefixSession(session, host, api)


class CommandContextApi:
    def __init__(self, wc: WorkspaceClient):
        self.wc = wc

    def create(self, cluster_id: str) -> str:
        self.wc.clusters.ensure_cluster_is_running(cluster_id)
        response = self.wc.command_execution.create_and_wait(
            cluster_id=cluster_id, language=Language.PYTHON
        )
        logger.info(f"Creating execution context response={response}")

        if response.status == ContextStatus.ERROR or not response.id:
            raise DbtRuntimeError(f"Error creating an execution context.\n {response}")
        self.wc.command_execution.wait_context_status_command_execution_running(
            cluster_id, response.id
        )
        return response.id

    def destroy(self, cluster_id: str, context_id: str) -> None:
        self.wc.command_execution.destroy(cluster_id, context_id)


class FolderApi(ABC):
    @abstractmethod
    def get_folder(self, catalog: str, schema: str) -> str:
        pass


# Use this for now to not break users
class SharedFolderApi(FolderApi):
    def get_folder(self, _: str, schema: str) -> str:
        return f"/Shared/dbt_python_models/{schema}"


class CurrUserApi:
    def __init__(self, wc: WorkspaceClient):
        self.wc = wc
        self._user: Optional[User] = None

    def get_username(self) -> str:
        if not self._user:
            self._user = self.wc.current_user.me()
        return self._user.user_name or ""

    def is_service_principal(self, username: str) -> bool:
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(uuid_pattern, username, re.IGNORECASE))


class UserFolderApi(FolderApi):
    def __init__(self, user_api: CurrUserApi):
        self.user_api = user_api

    def get_folder(self, catalog: str, schema: str) -> str:
        username = self.user_api.get_username()
        folder = f"/Users/{username}/dbt_python_models/{catalog}/{schema}"
        logger.debug(f"Using python model folder '{folder}'")

        return folder


class WorkspaceApi(DatabricksApi):
    def __init__(self, wc: WorkspaceClient, folder_api: FolderApi):
        self.wc = wc
        self.folder_api = folder_api

    def create_python_model_dir(self, catalog: str, schema: str) -> str:
        folder = self.folder_api.get_folder(catalog, schema)
        self.wc.workspace.mkdirs(folder)

        return folder

    def upload_notebook(self, path: str, compiled_code: str) -> None:
        self.wc.workspace.upload(
            path=path,
            content=BytesIO(compiled_code.encode()),
            format=ImportFormat.SOURCE,
            language=WorkspaceLanguage.PYTHON,
            overwrite=True,
        )


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

    def model_dump(self) -> dict[str, Any]:
        return {
            "commandId": self.command_id,
            "contextId": self.context_id,
            "clusterId": self.cluster_id,
        }


class CommandApi:
    def __init__(self, wc: WorkspaceClient, timeout: int):
        self.wc = wc
        self.timeout = timedelta(seconds=timeout)

    def execute(self, cluster_id: str, context_id: str, command: str) -> CommandExecution:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        response = self.wc.command_execution.execute_and_wait(
            cluster_id=cluster_id,
            context_id=context_id,
            command=command,
            language=Language.PYTHON,
            timeout=self.timeout,
        )
        if response.status == ContextStatus.ERROR or not response.id:
            raise DbtRuntimeError(f"Error creating a command.\n {response}")

        return CommandExecution(
            command_id=response.id, context_id=context_id, cluster_id=cluster_id
        )

    def cancel(self, command: CommandExecution) -> None:
        logger.debug(f"Cancelling command {command}")
        self.wc.command_execution.cancel_and_wait(
            cluster_id=command.cluster_id,
            command_id=command.command_id,
            context_id=command.context_id,
        )
        self.wc.command_execution.wait_command_status_command_execution_cancelled(
            command.cluster_id, command.command_id, command.cluster_id
        )

    def poll_for_completion(self, command: CommandExecution) -> None:
        response = (
            self.wc.command_execution.wait_command_status_command_execution_finished_or_error(
                command.cluster_id, command.command_id, command.cluster_id
            )
        )
        if response.status == CommandStatus.ERROR:
            raise DbtRuntimeError(f"Error running command.\n {response}")


class FromDictable(Protocol):
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        pass


T = TypeVar("T", bound=FromDictable)


def convert_sdk_element(klass: type[T], input: Optional[dict[str, Any]]) -> Optional[T]:
    return utils.if_some(input, lambda x: klass.from_dict(x))


def convert_sdk_list(klass: type[T], input: Optional[list[dict[str, Any]]]) -> Optional[list[T]]:
    return utils.if_some(input, lambda x: [klass.from_dict(y) for y in x])


class JobRunsApi:
    def __init__(self, wc: WorkspaceClient, timeout: int):
        self.wc = wc
        self.timeout = timedelta(seconds=timeout)

    def convert_to_sdk_types(self, job_settings: dict[str, Any]) -> dict[str, Any]:
        return {
            "access_control_list": convert_sdk_list(
                JobAccessControlRequest, job_settings.get("access_control_list")
            ),
            "budget_policy_id": job_settings.get("budget_policy_id"),
            "email_notifications": convert_sdk_element(
                JobEmailNotifications, job_settings.get("email_notifications")
            ),
            "environments": convert_sdk_list(JobEnvironment, job_settings.get("environments")),
            "git_source": convert_sdk_element(GitSource, job_settings),
            "health": convert_sdk_element(JobsHealthRules, job_settings.get("health")),
            "idempotency_token": job_settings.get("idempotency_token"),
            "notification_settings": convert_sdk_element(
                JobNotificationSettings, job_settings.get("notification_settings")
            ),
            "queue": convert_sdk_element(QueueSettings, job_settings.get("queue")),
            "run_as": convert_sdk_element(JobRunAs, job_settings.get("run_as")),
            "timeout_seconds": self.timeout.total_seconds(),
            "webhook_notifications": convert_sdk_element(
                WebhookNotifications, job_settings.get("webhook_notifications")
            ),
        }

    def submit(
        self, run_name: str, job_spec: dict[str, Any], **additional_job_settings: dict[str, Any]
    ) -> int:
        submit_response = self.wc.jobs.submit_and_wait(
            run_name=run_name,
            tasks=[SubmitTask.from_dict(job_spec)],
            **self.convert_to_sdk_types(additional_job_settings),
        )

        logger.debug(f"Job submission response={submit_response}")
        status = submit_response.status
        if (
            status
            and status.termination_details
            and status.termination_details.type != TerminationTypeType.SUCCESS
        ):
            raise DbtRuntimeError(f"Error submitting job run {run_name}:\n {status}")

        if not submit_response.run_id:
            raise DbtRuntimeError(f"Error submitting job run {run_name}: No id returned")
        return submit_response.run_id

    def poll_for_completion(self, run_id: int) -> None:
        run = self.wc.jobs.wait_get_run_job_terminated_or_skipped(run_id, self.timeout)
        if run.state and run.state.result_state != RunResultState.SUCCESS:
            logger.debug(f"Job run {run_id} failed.\n {run}")
            self._get_exception(run, run_id)

    def _get_exception(self, run: Run, run_id: int) -> None:
        try:
            run_id = utils.if_some(run.tasks, lambda x: x[0].run_id) or run_id
            output = self.wc.jobs.get_run_output(run_id)
            raise DbtRuntimeError(
                "Python model failed with traceback as:\n"
                "(Note that the line number here does not "
                "match the line number in your code due to dbt templating)\n"
                f"{output.error}\n"
                f"{utils.remove_ansi(output.error_trace or '')}"
            )

        except Exception as e:
            if isinstance(e, DbtRuntimeError):
                raise e
            else:
                result_state = utils.if_some(run.state, lambda s: s.result_state) or ""
                state_message = utils.if_some(run.state, lambda s: s.state_message) or ""
                raise DbtRuntimeError(
                    f"Python model run ended in state {result_state} "
                    f"with state_message\n{state_message}"
                )

    def cancel(self, run_id: int) -> None:
        logger.debug(f"Cancelling run id {run_id}")
        self.wc.jobs.cancel_run_and_wait(run_id)


class JobPermissionsApi:
    def __init__(self, wc: WorkspaceClient):
        self.wc = wc

    def set(self, job_id: str, access_control_list: list[dict[str, Any]]) -> None:
        response = self.wc.jobs.set_permissions(
            job_id,
            access_control_list=convert_sdk_list(JobAccessControlRequest, access_control_list),
        )

        logger.debug(f"Workflow permissions update response={response}")

    def get(self, job_id: str) -> dict[str, Any]:
        response = self.session.get(f"/{job_id}")

        if response.status_code != 200:
            raise DbtRuntimeError(
                f"Error fetching Databricks workflow permissions.\n {response.content!r}"
            )

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


class DatabricksApiClient:
    instance: Optional["DatabricksApiClient"] = None

    def __init__(
        self,
        wc: WorkspaceClient,
        session: Session,
        host: str,
        polling_interval: int,
        timeout: int,
        use_user_folder: bool,
    ):
        self.command_contexts = CommandContextApi(wc)
        self.curr_user = CurrUserApi(wc)
        if use_user_folder:
            self.folders: FolderApi = UserFolderApi(self.curr_user)
        else:
            self.folders = SharedFolderApi()
        self.workspace = WorkspaceApi(wc, self.folders)
        self.commands = CommandApi(wc, timeout)
        self.job_runs = JobRunsApi(session, host, polling_interval, timeout)
        self.workflows = WorkflowJobApi(session, host)
        self.workflow_permissions = JobPermissionsApi(session, host)

    @classmethod
    def create(
        cls,
        credentials: DatabricksCredentials,
        timeout: int,
        use_user_folder: bool = False,
    ) -> "DatabricksApiClient":
        if cls.instance:
            return cls.instance
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
        header_factory = credentials.authenticate().credentials_provider()  # type: ignore
        session.auth = BearerAuth(header_factory)

        session.headers.update({"User-Agent": user_agent, **http_headers})
        host = credentials.host

        assert host is not None, "Host must be set in the credentials"
        wc = DatabricksCredentialManager.create_from(credentials).api_client
        cls.instance = DatabricksApiClient(
            wc, session, host, polling_interval, timeout, use_user_folder
        )
        return cls.instance
