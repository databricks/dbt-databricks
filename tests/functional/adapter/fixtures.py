import pytest


class MaterializationV2Mixin:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_table_creation_v2": True}}
