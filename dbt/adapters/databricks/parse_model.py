import posixpath
from typing import Optional

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks import constants


def catalog_name(model: RelationConfig) -> Optional[str]:
    """
    The user has not set the catalog, which is common as it's the legacy behavior.
    We need to infer a catalog based on the model config since macros now expect it.
    Luckily, all catalog attribution in the legacy behavior is on the model.
    This means we can take a default catalog and rely on the model overrides to supply the rest.
    """
    return _get(model, CATALOG_INTEGRATION_MODEL_CONFIG_NAME) or constants.DEFAULT_CATALOG.name


def file_format(model: RelationConfig) -> Optional[str]:
    return _get(model, "file_format")


def location_path(model: RelationConfig) -> Optional[str]:
    if not model.config:
        return None

    if model.config.get("include_full_name_in_path"):
        return posixpath.join(model.database, model.schema, model.identifier)
    return model.identifier


def location_root(model: RelationConfig) -> Optional[str]:
    return _get(model, "location_root", case_sensitive=True)


def table_format(model: RelationConfig) -> Optional[str]:
    return _get(model, "table_format")


def _get(
    model: RelationConfig, setting: str, case_sensitive: Optional[bool] = False
) -> Optional[str]:
    if not model.config:
        return None

    if value := model.config.get(setting):
        return value if case_sensitive else value.casefold()

    return None
