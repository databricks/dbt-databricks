from importlib import metadata

import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)
from packaging import version

from tests.functional.adapter.fixtures import MaterializationV2Mixin
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


@pytest.mark.skipif(
    version.parse(dbt_version) < version.parse("1.9.0b1"),
    reason="Microbatch is not supported with this version of core",
)
class TestDatabricksMicrobatchV2(MaterializationV2Mixin, TestDatabricksMicrobatch):
    pass
