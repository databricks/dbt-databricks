import pytest


class MaterializationV2Mixin:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}


class ManagedIcebergMixin:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_managed_iceberg": True}}
