import json
from typing import Any, ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils.encoding import md5

from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)

FINGERPRINT_PROPERTY = "dbt.expectations_hash"
ON_VIOLATION_ACTIONS = ("warn", "drop", "fail")


class ExpectationsConfig(DatabricksComponentConfig):
    """Component encapsulating the expectations of a streaming table or materialized view."""

    expectations: list[dict[str, str]] = []
    fingerprint: Optional[str] = None

    @property
    def tblproperties(self) -> dict[str, str]:
        return {FINGERPRINT_PROPERTY: self.fingerprint} if self.fingerprint else {}

    def get_diff(self, other: "ExpectationsConfig") -> Optional["ExpectationsConfig"]:
        # Databricks returns expectation expressions rewritten, so they cannot be compared with
        # the model; the fingerprint stored alongside them is compared instead.
        if self.fingerprint != other.fingerprint:
            return self
        return None


class ExpectationsProcessor(DatabricksComponentProcessor[ExpectationsConfig]):
    name: ClassVar[str] = "expectations"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> ExpectationsConfig:
        table = results.get("show_tblproperties")
        if table:
            for row in table.rows:
                if str(row[0]) == FINGERPRINT_PROPERTY:
                    return ExpectationsConfig(fingerprint=str(row[1]))
        return ExpectationsConfig()

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ExpectationsConfig:
        expectations = base.get_config_value(relation_config, "expectations") or []
        if not isinstance(expectations, list):
            raise DbtRuntimeError(f"`expectations` must be a list; got {expectations}.")
        if not expectations:
            return ExpectationsConfig()

        normalized = [cls._normalize(expectation) for expectation in expectations]
        # A different fingerprint for the same expectations re-declares every streaming table and
        # rebuilds every materialized view that has them, so this must not change.
        fingerprint = md5(json.dumps(sorted(normalized, key=lambda e: e["name"]), sort_keys=True))
        return ExpectationsConfig(expectations=normalized, fingerprint=fingerprint)

    @staticmethod
    def _normalize(expectation: Any) -> dict[str, str]:
        if (
            not isinstance(expectation, dict)
            or not expectation.get("name")
            or not expectation.get("expression")
            or set(expectation) - {"name", "expression", "on_violation"}
        ):
            raise DbtRuntimeError(
                "Each expectation takes `name`, `expression` and optionally `on_violation`;"
                f" got {expectation}."
            )
        on_violation = str(expectation.get("on_violation") or "warn").lower()
        if on_violation not in ON_VIOLATION_ACTIONS:
            raise DbtRuntimeError(
                f"Invalid `on_violation` '{expectation['on_violation']}' for expectation"
                f" '{expectation['name']}'; expected `warn`, `drop`, or `fail`."
            )
        return {
            "name": str(expectation["name"]),
            "expression": str(expectation["expression"]),
            "on_violation": on_violation,
        }
