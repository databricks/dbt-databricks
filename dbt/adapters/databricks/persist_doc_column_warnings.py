"""Shared warning for documented columns absent from the relation.

Both the V1 persist_docs helper (``get_persist_doc_columns``) and the V2 changeset
helper (``ColumnCommentsConfig.get_diff``) need to surface the same user-facing
warning. On V1 incremental subsequent runs those two paths can both execute in a
single model materialization; dedupe so the message appears exactly once per
unique missing set **within that materialization**.

State is thread-local: dbt assigns each model to one worker thread, so parallel
runs do not clear or suppress each other's warnings. ``pre_model_hook`` resets
the cache at the start of each model on that thread.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence

from dbt.adapters.events.types import AdapterEventWarning
from dbt_common.events.functions import warn_or_error

_thread_state = threading.local()


def _emitted_keys() -> set[str]:
    keys = getattr(_thread_state, "emitted_missing_keys", None)
    if keys is None:
        keys = set()
        _thread_state.emitted_missing_keys = keys
    return keys


def reset_missing_persist_doc_column_warnings() -> None:
    """Clear the per-materialization dedupe cache for the current thread."""
    _thread_state.emitted_missing_keys = set()


def warn_missing_persist_doc_columns(missing: Sequence[str]) -> None:
    """Warn once per unique set of documented-but-absent column names (per thread)."""
    if not missing:
        return
    key = ", ".join(sorted(missing, key=str.lower))
    emitted = _emitted_keys()
    if key in emitted:
        return
    emitted.add(key)
    warn_or_error(
        AdapterEventWarning(
            base_msg=(
                "The following columns are specified in the schema but are not present "
                "in the database and will be skipped: " + ", ".join(missing)
            )
        )
    )
