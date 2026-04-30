import re
from enum import Enum
from typing import Any, ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.exceptions import DbtRuntimeError
from pydantic import model_validator

from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class RefreshMode(str, Enum):
    MANUAL = "manual"
    CRON = "cron"
    EVERY = "every"
    ON_UPDATE = "on_update"


CRON_REGEX = re.compile(r"^CRON '(.*)' AT TIME ZONE '(.*)'$")
EVERY_REGEX = re.compile(r"^EVERY (\d+) (HOURS?|DAYS?|WEEKS?)$", re.IGNORECASE)
TRIGGER_REGEX = re.compile(
    r"^TRIGGER ON UPDATE(?: AT MOST EVERY INTERVAL (\d+) SECONDS?)?$",
    re.IGNORECASE,
)

_QUANTITY_RE = re.compile(r"^\s*(\d+)\s+([A-Z]+)\s*$", re.IGNORECASE)
_SECONDS_PER_UNIT = {
    "SECOND": 1,
    "MINUTE": 60,
    "HOUR": 3600,
    "DAY": 86400,
    "WEEK": 604800,
}
_EVERY_UNITS = {"HOUR", "DAY", "WEEK"}


def _parse_quantity(value: str) -> tuple[int, str]:
    """Parse '<n> <unit>' (case-insensitive, singular or plural) into (n, singular_unit)."""
    match = _QUANTITY_RE.match(value)
    if not match:
        raise DbtRuntimeError(f"Cannot parse interval {value!r}; expected '<integer> <unit>'.")
    n, unit = int(match.group(1)), match.group(2).upper()
    singular = unit[:-1] if unit.endswith("S") else unit
    return n, singular


def _interval_seconds(value: str) -> int:
    n, singular = _parse_quantity(value)
    if singular not in _SECONDS_PER_UNIT:
        raise DbtRuntimeError(
            f"Unknown interval unit in {value!r};"
            f" supported: SECOND, MINUTE, HOUR, DAY, WEEK (singular or plural)."
        )
    return n * _SECONDS_PER_UNIT[singular]


def _every_canonical(value: str) -> tuple[int, str]:
    """Return (n, plural_unit) for an EVERY clause, e.g. '1 DAY' -> (1, 'DAYS')."""
    n, singular = _parse_quantity(value)
    if singular not in _EVERY_UNITS:
        raise DbtRuntimeError(
            f"Cannot parse `every` value {value!r}; expected '<integer> {{HOURS|DAYS|WEEKS}}'."
        )
    return n, singular + "S"


class RefreshConfig(DatabricksComponentConfig):
    """Component encapsulating the refresh schedule of a relation.

    The mode is derived from which discriminator field is set:
      - MANUAL    - no fields set
      - CRON      - `cron` set, optional `time_zone_value`
      - EVERY     - `every` set, e.g. "2 HOURS"
      - ON_UPDATE - `on_update=True`, optional `at_most_every` (e.g. "15 MINUTES")
    """

    cron: Optional[str] = None
    time_zone_value: Optional[str] = None
    every: Optional[str] = None
    on_update: bool = False
    at_most_every: Optional[str] = None

    # Render-time hint for the alter macro: True when both old and new states are scheduled
    # (emit ALTER); False for ADD or DROP. Excluded from __eq__ / __hash__ so it doesn't
    # affect identity.
    is_altered: bool = False

    @model_validator(mode="after")
    def _validate_mode_fields(self) -> "RefreshConfig":
        modes_set = [name for name, value in self._mode_signals() if value]
        if len(modes_set) > 1:
            raise DbtRuntimeError(
                f"Refresh schedule must specify at most one of cron / every / on_update;"
                f" got {modes_set}."
            )
        if self.time_zone_value is not None and self.cron is None:
            raise DbtRuntimeError("`time_zone_value` is only valid when `cron` is set.")
        if self.at_most_every is not None:
            if not self.on_update:
                raise DbtRuntimeError("`at_most_every` is only valid when `on_update` is True.")
            seconds = _interval_seconds(self.at_most_every)
            if seconds < 60:
                raise DbtRuntimeError(
                    f"`at_most_every` must be at least 60 seconds (1 minute);"
                    f" got {self.at_most_every!r} ({seconds}s)."
                )
        return self

    def _mode_signals(self) -> tuple[tuple[str, Any], ...]:
        return (
            ("cron", self.cron),
            ("every", self.every),
            ("on_update", self.on_update),
        )

    @property
    def mode(self) -> RefreshMode:
        if self.cron is not None:
            return RefreshMode.CRON
        if self.every is not None:
            return RefreshMode.EVERY
        if self.on_update:
            return RefreshMode.ON_UPDATE
        return RefreshMode.MANUAL

    @property
    def auto_refreshed(self) -> bool:
        """True for modes where Databricks auto-manages refresh and a manual REFRESH is a no-op."""
        return self.mode in (RefreshMode.EVERY, RefreshMode.ON_UPDATE)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RefreshConfig) or self.mode != other.mode:
            return False
        if self.mode == RefreshMode.MANUAL:
            return True
        if self.mode == RefreshMode.CRON:
            # Databricks treats no time zone as UTC.
            tz_self = (self.time_zone_value or "UTC").upper()
            tz_other = (other.time_zone_value or "UTC").upper()
            return self.cron == other.cron and tz_self == tz_other
        if self.mode == RefreshMode.EVERY:
            assert self.every is not None and other.every is not None
            return _every_canonical(self.every) == _every_canonical(other.every)
        if (self.at_most_every is None) != (other.at_most_every is None):
            return False
        if self.at_most_every is None:
            return True
        assert other.at_most_every is not None
        return _interval_seconds(self.at_most_every) == _interval_seconds(other.at_most_every)

    def __hash__(self) -> int:
        return hash(
            (self.cron, self.time_zone_value, self.every, self.on_update, self.at_most_every)
        )

    def get_diff(self, other: "RefreshConfig") -> Optional["RefreshConfig"]:
        if self == other:
            return None
        is_altered = self.mode != RefreshMode.MANUAL and other.mode != RefreshMode.MANUAL
        # model_construct skips re-validation; only is_altered changes, other fields stay valid.
        return self.model_construct(**{**self.model_dump(), "is_altered": is_altered})


class RefreshProcessor(DatabricksComponentProcessor[RefreshConfig]):
    name: ClassVar[str] = "refresh"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> RefreshConfig:
        table = results["describe_extended"]
        for row in table.rows:
            if row[0] != "Refresh Schedule":
                continue
            return cls._parse_schedule(row[1])

        raise DbtRuntimeError(
            "Could not find Refresh Schedule in describe extended."
            " Please file an issue at https://github.com/databricks/dbt-databricks/issues."
        )

    @staticmethod
    def _parse_schedule(value: str) -> RefreshConfig:
        if value == "MANUAL":
            return RefreshConfig()
        if (m := CRON_REGEX.match(value)) is not None:
            cron, time_zone_value = m.groups()
            return RefreshConfig(cron=cron, time_zone_value=time_zone_value)
        if (m := EVERY_REGEX.match(value)) is not None:
            n, unit = m.groups()
            return RefreshConfig(every=f"{n} {unit.upper()}")
        if (m := TRIGGER_REGEX.match(value)) is not None:
            seconds = m.group(1)
            if seconds is None:
                return RefreshConfig(on_update=True)
            return RefreshConfig(on_update=True, at_most_every=f"{seconds} SECOND")
        raise DbtRuntimeError(
            f"Could not parse refresh schedule from describe extended: {value!r}."
            " Please file an issue at https://github.com/databricks/dbt-databricks/issues."
        )

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> RefreshConfig:
        schedule = base.get_config_value(relation_config, "schedule")
        if not schedule:
            return RefreshConfig()
        if not isinstance(schedule, dict):
            raise DbtRuntimeError(f"`schedule` must be a dict; got {schedule!r}.")

        kwargs: dict[str, Any] = {
            field: schedule[field]
            for field in ("cron", "time_zone_value", "every", "on_update", "at_most_every")
            if field in schedule
        }

        if not kwargs:
            raise DbtRuntimeError(
                "Schedule config must contain one of `cron`, `every`, or `on_update`;"
                f" got {schedule}"
            )
        return RefreshConfig(**kwargs)
