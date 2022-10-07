from typing import Dict, Optional

from dbt.adapters.spark.python_submissions import (
    AllPurposeClusterPythonJobHelper,
    BaseDatabricksHelper,
    JobClusterPythonJobHelper,
)

from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.connections import DatabricksCredentials


class DbtDatabricksBasePythonJobHelper(BaseDatabricksHelper):
    credentials: DatabricksCredentials  # type: ignore[assignment]

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

        self.auth_header.update({"User-Agent": user_agent, **http_headers})

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
