from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field


DEFAULT_TIMEOUT = 60 * 60 * 24


class PythonJobConfig(BaseModel):
    name: Optional[str] = None
    email_notifications: Optional[Dict[str, Any]] = None
    webhook_notifications: Optional[Dict[str, Any]] = None
    notification_settings: Optional[Dict[str, Any]] = None
    timeout_seconds: Optional[int] = Field(None, gt=0)
    health: Optional[Dict[str, Any]] = None
    environments: Optional[List[Dict[str, Any]]] = None
    grants: Dict[str, List[Dict[str, str]]] = Field(exclude=True, default_factory=dict)
    existing_job_id: str = Field("", exclude=True)
    post_hook_tasks: List[Dict[str, Any]] = Field(exclude=True, default_factory=list)
    additional_task_settings: Dict[str, Any] = Field(exclude=True, default_factory=dict)


class PythonModelConfig(BaseModel):
    user_folder_for_python: bool = False
    timeout: int = Field(DEFAULT_TIMEOUT, gt=0)
    job_cluster_config: Dict[str, Any] = Field(default_factory=dict)
    access_control_list: List[Dict[str, str]] = Field(default_factory=list)
    packages: List[str] = Field(default_factory=list)
    index_url: Optional[str] = None
    additional_libs: List[Dict[str, Any]] = Field(default_factory=list)
    python_job_config: Optional[PythonJobConfig] = None
    cluster_id: Optional[str] = None
    http_path: Optional[str] = None
    create_notebook: bool = False


class ParsedPythonModel(BaseModel):
    catalog: str = Field("hive_metastore", alias="database")

    # Reserved name in Pydantic
    schema_: str = Field("default", alias="schema")

    identifier: str = Field(alias="alias")
    config: PythonModelConfig

    @property
    def run_name(self) -> str:
        return f"{self.catalog}-{self.schema_}-" f"{self.identifier}-{uuid.uuid4()}"

    class Config:
        allow_population_by_field_name = True
