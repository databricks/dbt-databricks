import pytest
from attr import dataclass
from dbt.adapters.databricks import connections
from dbt.adapters.databricks.connection import connection_utils
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt_common.exceptions import DbtRuntimeError


class TestGetMaxIdleTime:
    """Test the various cases for determining a specified warehouse."""

    errMsg = (
        "Compute resource foo does not exist or does not specify http_path, " "relation: a_relation"
    )

    def test_get_max_idle__no_compute(self):
        creds = DatabricksCredentials()

        # No node and nothing specified in creds
        time = connection_utils.get_max_idle_time(creds)
        assert connections.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__no_compute_override(self):
        creds = DatabricksCredentials(compute={"foo": {}})

        time = connection_utils.get_max_idle_time(creds, "foo")
        assert connections.DEFAULT_MAX_IDLE_TIME == time

    def test_get_max_idle__cred_default(self):
        creds_idle_time = 77
        creds = DatabricksCredentials(connect_max_idle=creds_idle_time)

        time = connection_utils.get_max_idle_time(creds)
        assert creds_idle_time == time

    def test_get_max_idle__cred_default_no_compute_override(self):
        creds_idle_time = 77
        creds = DatabricksCredentials(connect_max_idle=creds_idle_time, compute={"foo": {}})

        time = connection_utils.get_max_idle_time(creds, "foo")
        assert creds_idle_time == time

    def test_get_max_idle__compute_override(self):
        creds_idle_time = 88
        compute_idle_time = 77
        creds = DatabricksCredentials(
            connect_max_idle=creds_idle_time,
            compute={"foo": {"connect_max_idle": compute_idle_time}},
        )
        time = connection_utils.get_max_idle_time(creds, "foo")
        assert compute_idle_time == time

    def test_get_max_idle__invalid_string_override(self):
        creds_idle_time = "foo"
        compute_idle_time = "bar"
        creds = DatabricksCredentials(
            connect_max_idle=creds_idle_time,
            compute={"alternate_compute": {"connect_max_idle": compute_idle_time}},
        )

        with pytest.raises(DbtRuntimeError) as info:
            connection_utils.get_max_idle_time(creds, "alternate_compute")
        assert (
            f"{compute_idle_time} is not a valid value for connect_max_idle. "
            "Must be a number of seconds."
        ) in str(info.value)

    def test_get_max_idle__string_conversion_cred_default(self):
        creds_idle_time = "12"
        compute_idle_time = "34"
        creds = DatabricksCredentials(
            connect_max_idle=creds_idle_time,
            compute={"alternate_compute": {"connect_max_idle": compute_idle_time}},
        )

        time = connection_utils.get_max_idle_time(creds, None)
        assert int(creds_idle_time) == time

    def test_get_max_idle__string_conversion_compute_default(self):
        creds_idle_time = "12"
        compute_idle_time = "34"
        creds = DatabricksCredentials(
            connect_max_idle=creds_idle_time,
            compute={"alternate_compute": {"connect_max_idle": compute_idle_time}},
        )

        time = connection_utils.get_max_idle_time(creds, "alternate_compute")
        assert int(compute_idle_time) == time


class TestGetComputeName:
    @dataclass
    class TestModel:
        config: dict = {}

    @pytest.mark.parametrize(
        "context,expected",
        [
            (None, None),
            (TestModel(), None),
            (TestModel(config={"databricks_compute": "foo"}), "foo"),
        ],
    )
    def test_get_compute_name(self, context, expected):
        assert connection_utils.get_compute_name(context) is expected
