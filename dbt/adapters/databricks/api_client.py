import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from typing import Any, Optional, Protocol, TypeVar

from dbt_common.exceptions import DbtRuntimeError
from typing_extensions import Self

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import CommandStatus, ContextStatus, Language, ResultType
from databricks.sdk.service.iam import User
from databricks.sdk.service.jobs import (
    Continuous,
    CronSchedule,
    Format,
    GitSource,
    JobAccessControlRequest,
    JobCluster,
    JobDeployment,
    JobEditMode,
    JobEmailNotifications,
    JobEnvironment,
    JobNotificationSettings,
    JobParameterDefinition,
    JobRunAs,
    JobSettings,
    JobsHealthRules,
    QueueSettings,
    Run,
    RunResultState,
    SubmitTask,
    Task,
    TerminationTypeType,
    TriggerSettings,
    WebhookNotifications,
)
from databricks.sdk.service.pipelines import PipelineState
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.workspace import Language as WorkspaceLanguage
from dbt.adapters.databricks import utils
from dbt.adapters.databricks.credentials import (
    DatabricksCredentialManager,
    DatabricksCredentials,
)
from dbt.adapters.databricks.logging import logger


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
        self.wc.command_execution.destroy(cluster_id=cluster_id, context_id=context_id)


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


class WorkspaceApi:
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
        if response.status == CommandStatus.ERROR or not response.id:
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
        response = self.wc.command_execution.wait_command_status_command_execution_cancelled(
            command.cluster_id, command.command_id, command.cluster_id
        )
        if response.status not in [CommandStatus.FINISHED, CommandStatus.CANCELLED]:
            raise DbtRuntimeError(f"Command failed to cancel with {response}")

    def poll_for_completion(self, command: CommandExecution) -> None:
        response = (
            self.wc.command_execution.wait_command_status_command_execution_finished_or_error(
                command.cluster_id, command.command_id, command.cluster_id
            )
        )
        if response.status != CommandStatus.FINISHED:
            raise DbtRuntimeError(f"Command failed with {response}")

        if response.results and response.results.result_type == ResultType.ERROR:
            raise DbtRuntimeError(
                f"Python model failed with traceback as:\n"
                f"{utils.remove_ansi(response.results.cause or '')}"
            )


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

    def submit(
        self, run_name: str, job_spec: dict[str, Any], **additional_job_settings: dict[str, Any]
    ) -> int:
        submit_response = self.wc.jobs.submit_and_wait(
            run_name=run_name,
            tasks=[SubmitTask.from_dict(job_spec)],
            **self._convert_to_sdk_types(additional_job_settings),
        )

        logger.debug(f"Job submission response={submit_response}")
        status = submit_response.status
        if status and utils.if_some(
            status.termination_details,
            lambda x: x.type != TerminationTypeType.SUCCESS,  # type: ignore
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

    def _convert_to_sdk_types(self, job_settings: dict[str, Any]) -> dict[str, Any]:
        return {
            "access_control_list": convert_sdk_list(
                JobAccessControlRequest, job_settings.get("access_control_list")
            ),
            "budget_policy_id": job_settings.get("budget_policy_id"),
            "email_notifications": convert_sdk_element(
                JobEmailNotifications, job_settings.get("email_notifications")
            ),
            "environments": convert_sdk_list(JobEnvironment, job_settings.get("environments")),
            "git_source": convert_sdk_element(GitSource, job_settings.get("git_source")),
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

    def _get_exception(self, run: Run, run_id: int) -> None:
        try:
            run_id = utils.if_some(run.tasks, lambda x: x[0].run_id if x else None) or run_id  # type: ignore
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
                result_state = utils.if_some(run.state, lambda s: s.result_state) or ""  # type: ignore
                state_message = utils.if_some(run.state, lambda s: s.state_message) or ""  # type: ignore
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
        response = self.wc.jobs.get_permissions(job_id)

        return response.as_dict()


class WorkflowJobApi:
    def __init__(self, wc: WorkspaceClient, timeout: int):
        self.wc = wc
        self.timeout = timedelta(seconds=timeout)

    def search_by_name(self, job_name: str) -> list[dict[str, Any]]:
        return [j.as_dict() for j in self.wc.jobs.list(name=job_name)]

    def create(self, job_spec: dict[str, Any]) -> int:
        """
        :return: the job_id
        """
        response = self.wc.jobs.create(**self._convert_to_sdk_types(job_spec))

        job_id = response.job_id

        if not job_id:
            raise DbtRuntimeError(f"Error creating Workflow.\n {response}")
        logger.info(f"New workflow created with job id {job_id}")

        return job_id

    def update_job_settings(self, job_id: int, job_spec: dict[str, Any]) -> None:
        self.wc.jobs.reset(job_id, JobSettings.from_dict(self._convert_to_sdk_types(job_spec)))

    def run(self, job_id: int) -> int:
        response = self.wc.jobs.run_now_and_wait(job_id)

        if not response.run_id:
            raise DbtRuntimeError(f"Error running Workflow.\n {response}")

        return response.run_id

    def _convert_to_sdk_types(self, job_settings: dict[str, Any]) -> dict[str, Any]:
        return {
            "access_control_list": convert_sdk_list(
                JobAccessControlRequest, job_settings.get("access_control_list")
            ),
            "budget_policy_id": job_settings.get("budget_policy_id"),
            "continuous": convert_sdk_element(Continuous, job_settings.get("continuous")),
            "deployment": convert_sdk_element(JobDeployment, job_settings.get("deployment")),
            "description": job_settings.get("description"),
            "edit_mode": convert_sdk_element(JobEditMode, job_settings.get("edit_mode")),
            "email_notifications": convert_sdk_element(
                JobEmailNotifications, job_settings.get("email_notifications")
            ),
            "environments": convert_sdk_list(JobEnvironment, job_settings.get("environments")),
            "format": convert_sdk_element(Format, job_settings.get("format")),
            "git_source": convert_sdk_element(GitSource, job_settings.get("git_source")),
            "health": convert_sdk_element(JobsHealthRules, job_settings.get("health")),
            "job_clusters": convert_sdk_list(JobCluster, job_settings.get("job_clusters")),
            "max_concurrent_runs": job_settings.get("max_concurrent_runs"),
            "name": job_settings.get("name"),
            "notification_settings": convert_sdk_element(
                JobNotificationSettings, job_settings.get("notification_settings")
            ),
            "parameters": convert_sdk_list(JobParameterDefinition, job_settings.get("parameters")),
            "queue": convert_sdk_element(QueueSettings, job_settings.get("queue")),
            "run_as": convert_sdk_element(JobRunAs, job_settings.get("run_as")),
            "schedule": convert_sdk_element(CronSchedule, job_settings.get("schedule")),
            "tags": job_settings.get("tags"),
            "tasks": convert_sdk_list(Task, job_settings.get("tasks")),
            "timeout_seconds": self.timeout.total_seconds(),
            "trigger": convert_sdk_element(TriggerSettings, job_settings.get("trigger")),
            "webhook_notifications": convert_sdk_element(
                WebhookNotifications, job_settings.get("webhook_notifications")
            ),
        }


class DltPipelineApi:
    def __init__(self, wc: WorkspaceClient):
        self.wc = wc

    def poll_for_completion(self, pipeline_id: str) -> None:
        response = self.wc.pipelines.wait_get_pipeline_idle(pipeline_id)
        if response.state != PipelineState.IDLE:
            if response.cause:
                raise DbtRuntimeError(f"Pipeline {pipeline_id} failed: {response.cause}")
            else:
                latest_update = utils.if_some(
                    response.latest_updates,
                    lambda x: x[0] if x else None,  # type: ignore
                )
                last_error = self._get_update_error(pipeline_id, latest_update)
                raise DbtRuntimeError(f"Pipeline {pipeline_id} failed: {last_error}")

    def _get_update_error(self, pipeline_id: str, update_id: str) -> str:
        events = self.wc.pipelines.list_pipeline_events(pipeline_id)
        update_events = [
            e
            for e in events
            if e.event_type == "update_progress"
            and utils.if_some(e.origin, lambda x: x.update_id == update_id)  # type: ignore
        ]

        error_events = [e.error for e in update_events if e.error]

        msg = ""
        if error_events:
            msg = (
                utils.if_some(
                    error_events[0].exceptions,
                    lambda x: "\n".join(map(lambda y: y.message or "", x)),  # type: ignore
                )
                or ""
            )
        return msg


class DatabricksApiClient:
    instance: Optional["DatabricksApiClient"] = None

    def __init__(
        self,
        wc: WorkspaceClient,
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
        self.job_runs = JobRunsApi(wc, timeout)
        self.workflows = WorkflowJobApi(wc, timeout)
        self.workflow_permissions = JobPermissionsApi(wc)
        self.dlt_pipelines = DltPipelineApi(wc)

    @classmethod
    def create(
        cls,
        credentials: DatabricksCredentials,
        timeout: int,
        use_user_folder: bool = False,
    ) -> "DatabricksApiClient":
        if cls.instance:
            return cls.instance

        wc = DatabricksCredentialManager.create_from(credentials).api_client
        cls.instance = DatabricksApiClient(wc, timeout, use_user_folder)
        return cls.instance
