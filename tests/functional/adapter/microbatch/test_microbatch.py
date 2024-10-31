from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)
import pytest
from packaging import version
from importlib import metadata

from tests.functional.adapter.microbatch import fixtures

dbt_version = metadata.version("dbt-core")


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
