import uuid
from typing import Any, Optional

from pydantic import BaseModel, Field

DEFAULT_TIMEOUT = 60 * 60 * 24


class PythonJobConfig(BaseModel):
    """Pydantic model for config found in python_job_config."""

    name: Optional[str] = None
    grants: dict[str, list[dict[str, str]]] = Field(exclude=True, default_factory=dict)
    existing_job_id: str = Field("", exclude=True)
    post_hook_tasks: list[dict[str, Any]] = Field(exclude=True, default_factory=list)
    additional_task_settings: dict[str, Any] = Field(exclude=True, default_factory=dict)

    class Config:
        extra = "allow"


class PythonModelConfig(BaseModel):
    """
    Pydantic model for a Python model configuration.
    Includes some job-specific settings that are not yet part of PythonJobConfig.
    """

    user_folder_for_python: bool = False
    timeout: int = Field(DEFAULT_TIMEOUT, gt=0)
    job_cluster_config: dict[str, Any] = Field(default_factory=dict)
    access_control_list: list[dict[str, str]] = Field(default_factory=list)
    packages: list[str] = Field(default_factory=list)
    index_url: Optional[str] = None
    additional_libs: list[dict[str, Any]] = Field(default_factory=list)
    python_job_config: PythonJobConfig = Field(default_factory=lambda: PythonJobConfig(**{}))
    cluster_id: Optional[str] = None
    http_path: Optional[str] = None
    create_notebook: bool = False


class ParsedPythonModel(BaseModel):
    """Pydantic model for a Python model parsed from a dbt manifest"""

    catalog: str = Field("hive_metastore", alias="database")

    # Schema is a reserved name in Pydantic
    schema_: str = Field("default", alias="schema")

    identifier: str = Field(alias="alias")
    config: PythonModelConfig

    @property
    def run_name(self) -> str:
        return f"{self.catalog}-{self.schema_}-{self.identifier}-{uuid.uuid4()}"

    class Config:
        allow_population_by_field_name = True
