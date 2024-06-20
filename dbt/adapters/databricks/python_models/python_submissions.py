import uuid
from typing import Any
from typing import Dict
from typing import Optional

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.api_client import DatabricksApiClient
from dbt.adapters.databricks.api_client import CommandExecution
from dbt.adapters.databricks.python_models.run_tracking import PythonRunTracker


DEFAULT_TIMEOUT = 60 * 60 * 24


class BaseDatabricksHelper(PythonJobHelper):
    tracker = PythonRunTracker()

    def __init__(self, parsed_model: Dict, credentials: DatabricksCredentials) -> None:
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.database = parsed_model.get("database")
        self.parsed_model = parsed_model

        self.check_credentials()

        self.api_client = DatabricksApiClient.create(credentials, self.get_timeout())

    def get_timeout(self) -> int:
        timeout = self.parsed_model["config"].get("timeout", DEFAULT_TIMEOUT)
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        return timeout

    def check_credentials(self) -> None:
        self.credentials.validate_creds()

    def _update_with_acls(self, cluster_dict: dict) -> dict:
        acl = self.parsed_model["config"].get("access_control_list", None)
        if acl:
            cluster_dict.update({"access_control_list": acl})
        return cluster_dict

    def _submit_job(self, path: str, cluster_spec: dict) -> str:
        job_spec: Dict[str, Any] = {
            "task_key": "inner_notebook",
            "notebook_task": {
                "notebook_path": path,
            },
        }
        job_spec.update(cluster_spec)  # updates 'new_cluster' config

        # PYPI packages
        packages = self.parsed_model["config"].get("packages", [])

        # custom index URL or default
        index_url = self.parsed_model["config"].get("index_url", None)

        # additional format of packages
        additional_libs = self.parsed_model["config"].get("additional_libs", [])
        libraries = []

        for package in packages:
            if index_url:
                libraries.append({"pypi": {"package": package, "repo": index_url}})
            else:
                libraries.append({"pypi": {"package": package}})

        for lib in additional_libs:
            libraries.append(lib)

        job_spec.update({"libraries": libraries})
        run_name = f"{self.database}-{self.schema}-{self.identifier}-{uuid.uuid4()}"

        run_id = self.api_client.job_runs.submit(run_name, job_spec)
        self.tracker.insert_run_id(run_id)
        return run_id

    def _submit_through_notebook(self, compiled_code: str, cluster_spec: dict) -> None:
        workdir = self.api_client.workspace.create_python_model_dir(
            self.database or "hive_metastore", self.schema
        )
        file_path = f"{workdir}{self.identifier}"

        self.api_client.workspace.upload_notebook(file_path, compiled_code)

        # submit job
        run_id = self._submit_job(file_path, cluster_spec)
        try:
            self.api_client.job_runs.poll_for_completion(run_id)
        finally:
            self.tracker.remove_run_id(run_id)

    def submit(self, compiled_code: str) -> None:
        raise NotImplementedError(
            "BasePythonJobHelper is an abstract class and you should implement submit method."
        )


class JobClusterPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self) -> None:
        super().check_credentials()
        if not self.parsed_model["config"].get("job_cluster_config", None):
            raise ValueError(
                "`job_cluster_config` is required for the `job_cluster` submission method."
            )

    def submit(self, compiled_code: str) -> None:
        cluster_spec = {"new_cluster": self.parsed_model["config"]["job_cluster_config"]}
        self._submit_through_notebook(compiled_code, self._update_with_acls(cluster_spec))


class AllPurposeClusterPythonJobHelper(BaseDatabricksHelper):
    @property
    def cluster_id(self) -> Optional[str]:
        return self.parsed_model["config"].get(
            "cluster_id",
            self.credentials.extract_cluster_id(
                self.parsed_model["config"].get("http_path", self.credentials.http_path)
            ),
        )

    def check_credentials(self) -> None:
        super().check_credentials()
        if not self.cluster_id:
            raise ValueError(
                "Databricks `http_path` or `cluster_id` of an all-purpose cluster is required "
                "for the `all_purpose_cluster` submission method."
            )

    def submit(self, compiled_code: str) -> None:
        assert (
            self.cluster_id is not None
        ), "cluster_id is required for all_purpose_cluster submission method."
        if self.parsed_model["config"].get("create_notebook", False):
            config = {}
            if self.cluster_id:
                config["existing_cluster_id"] = self.cluster_id
            self._submit_through_notebook(compiled_code, self._update_with_acls(config))
        else:
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


class ServerlessClusterPythonJobHelper(BaseDatabricksHelper):
    def submit(self, compiled_code: str) -> None:
        self._submit_through_notebook(compiled_code, {})
