import pytest
from dbt.tests import util

from dbt.adapters.databricks.impl import USE_MANAGED_ICEBERG
from tests.functional.adapter.iceberg import fixtures


_iceberg_warning_text = USE_MANAGED_ICEBERG['description'][:25]

class TestNoManagedIcebergWarningOnDeltaProject:
    """A Delta-only project must not emit the `use_managed_iceberg` deprecation warning on
    `dbt run`."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"delta_model.sql": fixtures.basic_table}

    def test_use_managed_iceberg_warning_absent(self, project):
        _, logs = util.run_dbt_and_capture(["--debug", "run"])
        util.assert_message_in_logs(_iceberg_warning_text, logs, False)


@pytest.mark.skip_profile("databricks_cluster")
class TestManagedIcebergWarningPresentForIcebergModels:
    """The warning is the only actionable signal for users who have Iceberg
    models with the flag unset/false. It must still fire in that case."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_model.sql": fixtures.basic_iceberg_swap}

    def test_use_managed_iceberg_warning_present(self, project):
        _, logs = util.run_dbt_and_capture(["--debug", "run"])
        util.assert_message_in_logs(_iceberg_warning_text, logs)
