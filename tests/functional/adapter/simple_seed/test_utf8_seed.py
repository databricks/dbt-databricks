import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import MaterializationV2Mixin

UTF8_DATA = {
    "arabic": "مرحبا بالعالم",
    "greek": "Γειά σου Κόσμε",
    "chinese": "你好世界",
    "emoji": "Hello 🌍 World ✨",
    "mixed": "café naïve résumé — ",
}
SEED_CSV = "label,value\n" + "\n".join(f"{k},{v}" for k, v in UTF8_DATA.items())


class TestUtf8SeedRoundTrip:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"utf8_roundtrip.csv": SEED_CSV}

    def test_utf8_roundtrip(self, project):
        results = util.run_dbt(["seed"])
        assert len(results) == 1

        relation = util.relation_from_name(project.adapter, "utf8_roundtrip")
        rows = project.run_sql(f"select label, value from {relation}", fetch="all")
        assert dict(rows) == UTF8_DATA


class TestUtf8SeedRoundTripV2(TestUtf8SeedRoundTrip, MaterializationV2Mixin):
    pass
