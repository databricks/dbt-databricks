import pytest
from agate import Row
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests import util

from tests.functional.adapter.fixtures import RequiresDescribeAsJsonCapabilityMixin, RerunSafeMixin
from tests.functional.adapter.views import fixtures


class BaseUpdateView:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": fixtures.seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.schema_yml}


class BaseUpdateDescription(BaseUpdateView):
    def test_view_update_with_description(self, project):
        util.run_dbt(["build"])
        schema = util.read_file("models", "schema.yml")
        schema_2 = schema.replace("This is a view", "This is an updated view")
        util.write_file(schema_2, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            "describe extended {database}.{schema}.initial_view",
            fetch="all",
        )
        comments = [row[1] for row in results if row[0] == "Comment"]
        assert "This is an updated view" in comments


class BaseUpdateQuery(BaseUpdateView):
    def test_view_update_with_query(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.altered_view_sql, "models", "initial_view.sql")
        util.run_dbt(["run"])

        results = project.run_sql(
            "select * from {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0] == Row([1], ["id"])


class BaseUpdateNothing(BaseUpdateView):
    """This test is only to ensure that we don't error out due to lack of changes."""

    def test_view_update_nothing(self, project):
        util.run_dbt(["build"])
        util.run_dbt(["run"])

        results = project.run_sql(
            "describe extended {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0][2] == "This is the id column"


class BaseUpdateFullRefresh(BaseUpdateView):
    """Ensure that a full refresh forces a recreation even when there are no changes."""

    def test_view_update_full_refresh(self, project):
        util.run_dbt(["build"])

        project.run_sql(
            "alter view {database}.{schema}.initial_view "
            "set tblproperties ('triage_marker' = 'present')"
        )
        properties_before = dict(
            project.run_sql(
                "show tblproperties {database}.{schema}.initial_view",
                fetch="all",
            )
        )
        assert properties_before["triage_marker"] == "present"

        util.run_dbt(["run", "--full-refresh"])

        properties_after = dict(
            project.run_sql(
                "show tblproperties {database}.{schema}.initial_view",
                fetch="all",
            )
        )
        assert "triage_marker" not in properties_after


class BaseUpdateTblProperties(BaseUpdateView):
    def test_view_update_tblproperties(self, project):
        util.run_dbt(["build"])
        schema = util.read_file("models", "schema.yml")
        schema_2 = schema.replace("key: value", "key: value2")
        util.write_file(schema_2, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            "show tblproperties {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0][1] == "value2"


class BaseUpdateColumnComments(BaseUpdateView):
    def test_view_update_with_column_comments(self, project):
        util.run_dbt(["build"])
        schema = util.read_file("models", "schema.yml")
        schema_2 = schema.replace("This is the id column", "This is an id column")
        util.write_file(schema_2, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            "describe extended {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0][2] == "This is an id column"


class BaseUpdateQueryPreservesColumnComments(BaseUpdateView):
    """Regression for #1357: ALTER VIEW AS wipes column comments; they must be reapplied."""

    def test_view_update_query_preserves_column_comments(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.altered_view_sql, "models", "initial_view.sql")
        util.run_dbt(["run"])

        results = project.run_sql(
            "describe extended {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0][2] == "This is the id column"

    def test_view_update_query_drops_documented_column(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.altered_view_sql, "models", "initial_view.sql")
        util.write_file(fixtures.stale_comments_schema_yml, "models", "schema.yml")
        util.run_dbt(["run"])

        columns = project.run_sql("describe {database}.{schema}.initial_view", fetch="all")
        assert [(column.col_name, column.comment) for column in columns] == [
            ("id", "This is an id column")
        ]


class BaseRemoveTags(BaseUpdateView):
    def test_view_update_remove_tags(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.no_tag_schema_yml, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            f"""
            SELECT TAG_NAME, TAG_VALUE FROM {project.database}.information_schema.table_tags
            WHERE schema_name = '{project.test_schema}' AND table_name = 'initial_view'
            """,
            fetch="all",
        )

        # We remove the tag from the schema.yml, but this is an "add only" config so
        # we don't expect Databricks state to be updated. We still test this path
        # to ensure we don't get a "main is not being called during running model" error.
        assert len(results) == 1
        assert results[0][0] == "tag1"
        assert results[0][1] == "value1"


class BaseAddTags(BaseUpdateView):
    def test_view_update_add_tags(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.two_tag_schema_yml, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            f"""
            SELECT TAG_NAME, TAG_VALUE FROM {project.database}.information_schema.table_tags
            WHERE schema_name = '{project.test_schema}' AND table_name = 'initial_view'
            ORDER BY TAG_NAME
            """,
            fetch="all",
        )

        assert len(results) == 2
        assert results[0][0] == "tag1"
        assert results[0][1] == "value1"
        assert results[1][0] == "tag2"
        assert results[1][1] == "value2"


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterAddTags(BaseAddTags):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterDescription(BaseUpdateDescription):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterDescriptionDescribeJsonOn(
    RequiresDescribeAsJsonCapabilityMixin, TestUpdateViewViaAlterDescription
):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_describe_as_json_for_relation_metadata": True},
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterNothing(BaseUpdateNothing):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterFullRefresh(BaseUpdateFullRefresh):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterQuery(BaseUpdateQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterTblproperties(BaseUpdateTblProperties):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterColumnComments(RerunSafeMixin, BaseUpdateColumnComments):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.altered_view_sql, "schema.yml": fixtures.schema_yml}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("initial_view", "seed")

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterQueryPreservesColumnComments(
    RerunSafeMixin, BaseUpdateQueryPreservesColumnComments
):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("initial_view",)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterInheritedParquetColumnComments(
    TestUpdateViewViaAlterQueryPreservesColumnComments
):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
                "+file_format": "parquet",
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterRemoveTags(BaseRemoveTags):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewSafeReplaceDescription(BaseUpdateDescription):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewSafeReplaceQuery(BaseUpdateQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewSafeReplaceTblproperties(BaseUpdateTblProperties):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateUnsafeReplaceDescription(BaseUpdateDescription):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": False,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateUnsafeReplaceQuery(BaseUpdateQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": False,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateUnsafeReplaceTblproperties(BaseUpdateTblProperties):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": False,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


class HiveBaseUpdateDescription(BaseUpdateDescription):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.hive_schema_yml}


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateViewSafeReplaceDescription(HiveBaseUpdateDescription):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateUnsafeReplaceDescription(HiveBaseUpdateDescription):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": False,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


class HiveBaseUpdateQuery(BaseUpdateQuery):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.hive_schema_yml}


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateViewSafeReplaceQuery(HiveBaseUpdateQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateUnsafeReplaceQuery(HiveBaseUpdateQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": False,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


class HiveBaseUpdateTblproperties(BaseUpdateTblProperties):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.hive_schema_yml}


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateViewSafeReplaceTblproperties(HiveBaseUpdateTblproperties):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateUnsafeReplaceTblproperties(HiveBaseUpdateTblproperties):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+use_safer_relation_operations": False,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


class HiveViewUpdateViaAlterError(BaseUpdateView):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.hive_schema_yml}

    def test_view_update_via_alter_errors_on_hive(self, project):
        util.run_dbt(["build"])
        # ALTER VIEW is unavailable in the Hive metastore, so the second run must
        # fail instead of attempting an in-place update.
        failed = util.run_dbt(["run"], expect_pass=False)
        assert failed.results[0].status == RunStatus.Error


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveViewUpdateViaAlterError(HiveViewUpdateViaAlterError):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
            },
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterUpstreamSchema(RerunSafeMixin, BaseUpdateView):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("initial_view", "seed")

    @pytest.fixture(autouse=True)
    def reset_seed_file(self, project):
        util.write_file(fixtures.seed_csv, "seeds", "seed.csv")

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    @pytest.mark.parametrize(
        "view_sql",
        [
            pytest.param(fixtures.view_sql, id="star"),
            pytest.param(fixtures.qualified_star_view_sql, id="qualified-star"),
        ],
    )
    def test_view_update_with_upstream_schema_change(self, project, view_sql):
        util.write_file(view_sql, "models", "initial_view.sql")
        util.run_dbt(["build"])
        util.write_file(fixtures.seed_with_extra_csv, "seeds", "seed.csv")
        util.run_dbt(["seed", "--full-refresh"])
        util.run_dbt(["run"])

        columns = project.run_sql("describe {database}.{schema}.initial_view", fetch="all")

        assert [column.col_name for column in columns] == ["id", "msg", "extra"]
        assert columns[0][2] == "This is the id column"


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterValidation(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_view.sql": fixtures.contract_view_sql,
            "schema.yml": fixtures.contract_schema_yml,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("contract_view",)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {"+view_update_via_alter": True},
        }

    def test_contract_violation_blocks_alter(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.contract_violating_view_sql, "models", "contract_view.sql")

        failed = util.run_dbt(["run"], expect_pass=False)

        assert failed.results[0].status == RunStatus.Error
        rows = project.run_sql("select id from {database}.{schema}.contract_view", fetch="all")
        assert rows == [Row([1], ["id"])]

    def test_contract_violation_blocks_no_op(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.contract_changed_type_schema_yml, "models", "schema.yml")

        failed = util.run_dbt(["run"], expect_pass=False)

        assert failed.results[0].status == RunStatus.Error

    @pytest.mark.parametrize(
        "schema_yml",
        [
            pytest.param(fixtures.column_mask_schema_yml, id="column-mask"),
            pytest.param(fixtures.row_filter_schema_yml, id="row-filter"),
        ],
    )
    def test_unsupported_config_rejected(self, project, schema_yml):
        util.run_dbt(["run"])
        util.write_file(schema_yml, "models", "schema.yml")

        failed = util.run_dbt(["run"], expect_pass=False)

        assert failed.results[0].status == RunStatus.Error


@pytest.mark.skip_profile("databricks_cluster")
class TestViewLifecycleIgnoresMaterializationFlag(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+grants": {"select": ["account users"]}}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": fixtures.seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "altered_view.sql": fixtures.altered_lifecycle_view_sql,
            "replaced_view.sql": fixtures.replaced_lifecycle_view_sql,
        }

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("altered_view", "replaced_view")

    def test_alter_opt_in_selects_the_path(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])
        for name in ("altered_view", "replaced_view"):
            relation = util.relation_from_name(project.adapter, name)
            with util.get_connection(project.adapter):
                comment_sql = project.adapter.execute_macro(
                    "comment_on_column_sql",
                    kwargs={
                        "column_path": f"{relation.render()}.id",
                        "escaped_comment": "external comment",
                    },
                )
            project.run_sql(comment_sql)

        util.run_dbt(["run"])

        for name, comment in (("altered_view", "external comment"), ("replaced_view", None)):
            columns = project.run_sql(f"describe {{database}}.{{schema}}.{name}", fetch="all")
            assert [(column.col_name, column.comment) for column in columns] == [
                ("id", comment),
                ("msg", None),
            ]
            grants = project.run_sql(
                "select grantee, privilege_type from {database}.information_schema.table_privileges"
                f" where table_schema = '{{schema}}' and table_name = '{name}'"
                " and inherited_from = 'NONE'",
                fetch="all",
            )
            assert ("account users", "SELECT") in {(row[0], row[1]) for row in grants}


@pytest.mark.skip_profile("databricks_cluster")
class TestViewLifecycleMaterializationFlagOn(TestViewLifecycleIgnoresMaterializationFlag):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+grants": {"select": ["account users"]}},
        }


class TestCreateViewWrapperOverride(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"create_view_override.sql": fixtures.create_view_wrapper_override_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"override_view.sql": fixtures.override_view_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("override_view",)

    def test_override_builds_every_run(self, project):
        markers = set()
        for args in (["run"], ["run"], ["run", "--full-refresh"]):
            util.run_dbt(args)
            rows = project.run_sql(
                "select override_marker from {database}.{schema}.override_view", fetch="all"
            )
            markers.add(rows[0][0])

        assert len(markers) == 3


class TestCreateViewAsOverride(TestCreateViewWrapperOverride):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"create_view_override.sql": fixtures.create_view_as_override_sql}
