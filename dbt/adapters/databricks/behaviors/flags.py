"""
Behavior flags for the Databricks adapter.

These flags control various adapter behaviors and can be enabled/disabled
through dbt configuration.
"""

from dbt_common.behavior_flags import BehaviorFlag

# Information schema behavior flag
USE_INFO_SCHEMA_FOR_COLUMNS = BehaviorFlag(
    name="use_info_schema_for_columns",
    default=False,
    description=(
        "Use info schema to gather column information to ensure complex types are not truncated."
        "  Incurs some overhead, so disabled by default."
    ),
)  # type: ignore[typeddict-item]

# Python model folder behavior flag
USE_USER_FOLDER_FOR_PYTHON = BehaviorFlag(
    name="use_user_folder_for_python",
    default=True,
    description=(
        "Use the user's home folder for uploading python notebooks."
        "  Shared folder use is deprecated due to governance concerns."
    ),
)  # type: ignore[typeddict-item]

# Materialization version behavior flag
USE_MATERIALIZATION_V2 = BehaviorFlag(
    name="use_materialization_v2",
    default=False,
    description=(
        "Use revamped materializations based on separating create and insert."
        "  This allows more performant column comments, as well as new column features."
    ),
)  # type: ignore[typeddict-item]

# List of all behavior flags for easy access
ALL_BEHAVIOR_FLAGS = [
    USE_INFO_SCHEMA_FOR_COLUMNS,
    USE_USER_FOLDER_FOR_PYTHON,
    USE_MATERIALIZATION_V2,
]
