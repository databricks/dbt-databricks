from importlib.metadata import version


def is_pydantic_v1() -> bool:
    """Check if the installed version of pydantic is v1."""
    try:
        pydantic_version = version("pydantic")
        major = int(pydantic_version.split(".")[0])
        return major < 2
    except Exception:
        # If we can't determine the version, assume v1 for compatibility
        # See: https://github.com/databricks/dbt-databricks/pull/976#issuecomment-2748680090
        return True


PYDANTIC_IS_V1 = is_pydantic_v1()
