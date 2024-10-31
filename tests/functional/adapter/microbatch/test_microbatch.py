from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)
import pytest
from packaging import version
import pkg_resources

from tests.functional.adapter.microbatch import fixtures

dbt_version = pkg_resources.get_distribution("dbt-core").version


@pytest.mark.skipif(
    version.parse(dbt_version) < version.parse("1.9.0b1"),
    reason="Microbatch is not supported with this version of core",
)
class TestDatabricksMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def models(self, microbatch_model_sql, input_model_sql):
        return {
            "schema.yml": fixtures.schema,
            "input_model.sql": input_model_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }
