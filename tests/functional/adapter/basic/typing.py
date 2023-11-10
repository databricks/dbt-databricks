from dbt.tests.util import AnyInteger


class StatsLikeDict:
    """Any stats-like dict. Use this in assert calls"""

    def __eq__(self, other):
        return (
            isinstance(other, dict)
            and "has_stats" in other
            and (
                other["has_stats"]
                == {
                    "id": "has_stats",
                    "label": "Has Stats?",
                    "value": AnyInteger(),
                    "description": "Indicates whether there are statistics for this table",
                    "include": False,
                }
            )
        )


class AnyLongType:
    """Allows bigint and long to be treated equivalently"""

    def __eq__(self, other):
        return isinstance(other, str) and other in ("bigint", "long")
