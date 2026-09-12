from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestFormatAllowsCreateOrReplace(MacroTestBase):
    """The predicate that decides whether a full refresh can use `create or replace table`
    instead of dropping the existing relation first (issue #1662)."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replaceable_format.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    def _catalog_relation(self, table_format="default", file_format="delta"):
        catalog_relation = Mock()
        catalog_relation.table_format = table_format
        catalog_relation.file_format = file_format
        return catalog_relation

    def _existing_relation(self, is_delta=False, is_iceberg=False):
        existing_relation = Mock()
        existing_relation.is_delta = is_delta
        existing_relation.is_iceberg = is_iceberg
        return existing_relation

    def run_predicate(self, template_bundle, catalog_relation, existing_relation, managed_iceberg):
        template_bundle.context["adapter"].get_behavior_flag_no_warn = Mock(
            side_effect=lambda name: managed_iceberg if name == "use_managed_iceberg" else False
        )
        return self.run_macro_raw(
            template_bundle.template,
            "format_allows_create_or_replace",
            catalog_relation,
            existing_relation,
        ).strip()

    def test_delta_target_on_delta_relation(self, template_bundle):
        result = self.run_predicate(
            template_bundle,
            self._catalog_relation(),
            self._existing_relation(is_delta=True),
            managed_iceberg=False,
        )
        assert result == "True"

    def test_delta_target_on_iceberg_relation(self, template_bundle):
        """Provider changed under the model, so the table has to be dropped."""
        result = self.run_predicate(
            template_bundle,
            self._catalog_relation(),
            self._existing_relation(is_iceberg=True),
            managed_iceberg=False,
        )
        assert result == "False"

    def test_managed_iceberg_target_on_iceberg_relation(self, template_bundle):
        """The case from #1662: an Iceberg model keeps `file_format` at delta, so keying the
        Iceberg arm off `file_format` never matched and the full refresh dropped the table."""
        result = self.run_predicate(
            template_bundle,
            self._catalog_relation(table_format="iceberg"),
            self._existing_relation(is_iceberg=True),
            managed_iceberg=True,
        )
        assert result == "True"

    def test_uniform_target_on_delta_relation(self, template_bundle):
        """`table_format: iceberg` without the behavior flag writes a Delta table with UniForm
        properties, so the relation stays Delta and remains replaceable."""
        result = self.run_predicate(
            template_bundle,
            self._catalog_relation(table_format="iceberg"),
            self._existing_relation(is_delta=True),
            managed_iceberg=False,
        )
        assert result == "True"

    def test_managed_iceberg_target_on_delta_relation(self, template_bundle):
        """A project that has just switched the flag on still has a Delta table. `create or
        replace` cannot change a table's provider -- Databricks rejects it with
        MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED -- so this has to drop and recreate even though
        `file_format` still reads delta for a managed Iceberg model."""
        result = self.run_predicate(
            template_bundle,
            self._catalog_relation(table_format="iceberg"),
            self._existing_relation(is_delta=True),
            managed_iceberg=True,
        )
        assert result == "False"

    def test_non_delta_file_format(self, template_bundle):
        result = self.run_predicate(
            template_bundle,
            self._catalog_relation(file_format="parquet"),
            self._existing_relation(is_delta=True),
            managed_iceberg=False,
        )
        assert result == "False"
