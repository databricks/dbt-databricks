"""
Unit tests for DatabricksDescribeJsonMetadata parser.

Tests the parsing of DESCRIBE TABLE EXTENDED ... AS JSON responses into
agate Tables that match the format expected by existing processors.
"""

from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.databricks.constraints import (
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from dbt.adapters.databricks.impl import DatabricksDescribeJsonMetadata
from dbt.adapters.databricks.relation_configs.column_mask import (
    ColumnMaskConfig,
    ColumnMaskProcessor,
)
from dbt.adapters.databricks.relation_configs.constraints import (
    ConstraintsConfig,
    ConstraintsProcessor,
)
from dbt.adapters.databricks.relation_configs.query import QueryConfig, QueryProcessor
from dbt.adapters.databricks.relation_configs.row_filter import (
    RowFilterConfig,
    RowFilterProcessor,
)

# Fixtures: minimal JSON samples with only fields relevant to parsing.


EMAIL_ADDRESSES_JSON = {
    "columns": [
        {"name": "address_id", "nullable": False},
        {"name": "remote_user_id", "nullable": True},
        {"name": "email_address", "nullable": True},
    ],
    "table_constraints": (
        "[(email_ad_pk,PRIMARY KEY (`address_id`)),"
        " (email_fk,FOREIGN KEY (`remote_user_id`)"
        " REFERENCES `main`.`default`.`users` (`user_id`))]"
    ),
}

COLUMN_MASK_JSON = {
    "column_masks": [
        {
            "column_name": "phone_number",
            "function_name": {
                "catalog_name": "main",
                "schema_name": "db",
                "function_name": "mask_phone",
            },
            "using_column_names": ["city"],
        }
    ],
}

ROW_FILTER_JSON = {
    "table_name": "table_with_row_filter",
    "catalog_name": "default_catalog",
    "schema_name": "default",
    "row_filter": {
        "function_name": {
            "catalog_name": "default_catalog",
            "schema_name": "default",
            "function_name": "filter_by_region",
        },
        "column_names": ["region"],
    },
}

ROW_FILTER_MULTI_COLUMN_JSON = {
    "table_name": "table_with_row_filter",
    "catalog_name": "default_catalog",
    "schema_name": "default",
    "row_filter": {
        "function_name": {
            "catalog_name": "default_catalog",
            "schema_name": "default",
            "function_name": "filter_by_dept_and_region",
        },
        "column_names": ["department", "region"],
    },
}


MATERIALIZED_VIEW_JSON = {
    "view_text": "SELECT id, name FROM main.default.source_table",
}

REGULAR_VIEW_JSON = {
    "view_text": "SELECT id, name FROM main.default.other_table",
}

PLAIN_TABLE_JSON = {
    "columns": [
        {"name": "id", "nullable": True},
        {"name": "value", "nullable": True},
    ],
}


COMPOSITE_PK_JSON = {
    "columns": [
        {"name": "id", "nullable": False},
        {"name": "name", "nullable": False},
        {"name": "value", "nullable": True},
    ],
    "table_constraints": "[(id_name_pk,PRIMARY KEY (`id`, `name`))]",
}

COMPOSITE_FK_JSON = {
    "columns": [
        {"name": "id", "nullable": True},
        {"name": "ref_id", "nullable": True},
        {"name": "ref_name", "nullable": True},
    ],
    "table_constraints": (
        "[(fk_pk,PRIMARY KEY (`id`)),"
        " (child_fk,FOREIGN KEY (`ref_id`, `ref_name`)"
        " REFERENCES `main`.`default`.`parents` (`id`, `name`))]"
    ),
}

ALL_FIELDS_JSON = {
    "table_name": "source",
    "catalog_name": "main",
    "schema_name": "default",
    "columns": [
        {"name": "id", "nullable": False},
        {"name": "secret", "nullable": True},
    ],
    "table_constraints": (
        "[(pk1,PRIMARY KEY (`id`)),"
        " (fk1,FOREIGN KEY (`id`)"
        " REFERENCES `main`.`default`.`other` (`other_id`))]"
    ),
    "column_masks": [
        {
            "column_name": "secret",
            "function_name": {
                "catalog_name": "main",
                "schema_name": "db",
                "function_name": "mask_secret",
            },
            "using_column_names": ["id"],
        }
    ],
    "row_filter": {
        "function_name": {
            "catalog_name": "main",
            "schema_name": "db",
            "function_name": "filter_secret",
        },
        "column_names": ["id"],
    },
    "view_text": "SELECT id, secret FROM main.default.source",
}

MIXED_PK_FK_JSON = {
    "columns": [
        {"name": "id", "nullable": False},
        {"name": "ref_id", "nullable": True},
    ],
    "table_constraints": (
        "[(pk1,PRIMARY KEY (`id`)),"
        " (fk1,FOREIGN KEY (`ref_id`)"
        " REFERENCES `main`.`default`.`other` (`other_id`))]"
    ),
}


class TestParsePrimaryKeyConstraints:
    def test_single_primary_key(self):
        """Test PRIMARY KEY parsing with a single primary key constraint."""
        json_metadata = {"table_constraints": "[(pk1,PRIMARY KEY (`address_id`))]"}
        result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints(json_metadata)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "pk1"
        assert result.rows[0]["constraint_name"] == "pk1"
        assert result.rows[0][1] == "address_id"
        assert result.rows[0]["column_name"] == "address_id"

    def test_no_primary_key(self):
        """Test PRIMARY KEY parsing with no primary key constraints."""
        json_metadata = {
            "table_constraints": (
                "[(fk1,FOREIGN KEY (`ref_id`) REFERENCES `main`.`default`.`t` (`id`))]"
            )
        }
        result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints(json_metadata)
        assert len(result.rows) == 0

    def test_no_table_constraints_field(self):
        """Test PRIMARY KEY parsing with no table_constraints field."""
        result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints({})
        assert len(result.rows) == 0

    def test_empty_string(self):
        """Test PRIMARY KEY parsing with an empty string."""
        result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints(
            {"table_constraints": ""}
        )
        assert len(result.rows) == 0

    def test_spaces(self):
        """Test PRIMARY KEY parsing is robust to excessive spaces between 'PRIMARY' and 'KEY'."""
        for num_extra_spaces in range(0, 40):
            es = " " * num_extra_spaces  # extra spaces
            constraint_entry = f"{es}({es}pk1{es},{es}PRIMARY {es}KEY{es}({es}`col_1`{es}){es}){es}"
            json_metadata = {"table_constraints": f"[{constraint_entry}]"}
            result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints(json_metadata)
            assert len(result.rows) == 1
            row = result.rows[0]
            assert row[0] == "pk1"
            assert row["constraint_name"] == "pk1"
            assert row[1] == "col_1"
            assert row["column_name"] == "col_1"

    def test_many_constraints(self):
        """Test PRIMARY KEY constraint parsing with many constraints in one string."""
        constraint_count = 20
        constraint_entries = [
            f"(pk{i},PRIMARY KEY (`col_{i}`))" for i in range(1, constraint_count + 1)
        ]
        json_metadata = {"table_constraints": f"[{', '.join(constraint_entries)}]"}
        result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints(json_metadata)
        assert len(result.rows) == constraint_count
        for row_index in range(constraint_count):
            expected_constraint_name = f"pk{row_index + 1}"
            expected_column_name = f"col_{row_index + 1}"
            row = result.rows[row_index]
            assert row[0] == expected_constraint_name
            assert row["constraint_name"] == expected_constraint_name
            assert row[1] == expected_column_name
            assert row["column_name"] == expected_column_name

    def test_composite_with_many_columns(self):
        """Test composite PRIMARY KEY with 1 to 20 columns."""
        for num_cols in range(1, 21):
            cols = ", ".join(f"`col_{i}`" for i in range(1, num_cols + 1))
            json_metadata = {"table_constraints": f"[(pk1,PRIMARY KEY ({cols}))]"}
            result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints(json_metadata)
            assert len(result.rows) == num_cols
            for i in range(num_cols):
                assert result.rows[i][0] == "pk1"
                assert result.rows[i][1] == f"col_{i + 1}"

    def test_underscores_on_names(self):
        """
        Test that PRIMARY KEY parsing works when table/column names in constraints
        are qualified with varying numbers of leading/trailing underscores.
        """
        for i in range(0, 20):
            usc = "_" * i  # underscores
            column_name = f"{usc}id{usc}"
            constraint_entry = f"(pk1,PRIMARY KEY (`{column_name}`))"

            json_metadata = {"table_constraints": f"[{constraint_entry}]"}
            result = DatabricksDescribeJsonMetadata.parse_primary_key_constraints(json_metadata)
            assert len(result.rows) == 1
            row = result.rows[0]
            assert row[0] == "pk1"
            assert row["constraint_name"] == "pk1"
            assert row[1] == column_name
            assert row["column_name"] == column_name


class TestParseForeignKeyConstraints:
    def test_single_column_foreign_key(self):
        """Test FOREIGN KEY parsing with a single foreign key constraint."""
        json_metadata = {
            "table_constraints": (
                "[(fk1,FOREIGN KEY (`ref_id`) REFERENCES `main`.`default`.`users` (`user_id`))]"
            )
        }
        result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(json_metadata)
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row[0] == "fk1"
        assert row["constraint_name"] == "fk1"
        assert row[1] == "ref_id"
        assert row["from_column"] == "ref_id"
        assert row[2] == "main"
        assert row["to_catalog"] == "main"
        assert row[3] == "default"
        assert row["to_schema"] == "default"
        assert row[4] == "users"
        assert row["to_table"] == "users"
        assert row[5] == "user_id"
        assert row["to_column"] == "user_id"

    def test_composite_foreign_key(self):
        """Test FOREIGN KEY parsing many columns."""
        for num_cols in range(1, 21):
            from_cols = ", ".join(f"`from_{i}`" for i in range(1, num_cols + 1))
            to_cols = ", ".join(f"`to_{i}`" for i in range(1, num_cols + 1))
            json_metadata = {
                "table_constraints": (
                    f"[(cfk,FOREIGN KEY ({from_cols})"
                    f" REFERENCES `main`.`default`.`parents` ({to_cols}))]"
                )
            }
            result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(json_metadata)
            assert len(result.rows) == num_cols
            for i in range(num_cols):
                row = result.rows[i]
                assert row[0] == "cfk"
                assert row["constraint_name"] == "cfk"
                assert row[1] == f"from_{i + 1}"
                assert row["from_column"] == f"from_{i + 1}"
                assert row[2] == "main"
                assert row[3] == "default"
                assert row[4] == "parents"
                assert row[5] == f"to_{i + 1}"
                assert row["to_column"] == f"to_{i + 1}"

    def test_schema_with_hyphens(self):
        """Test FOREIGN KEY parsing when the referenced schema contains hyphens."""
        json_metadata = {
            "table_constraints": (
                "[(fk1,FOREIGN KEY (`ref_id`) REFERENCES `main`.`my-schema`.`users` (`user_id`))]"
            )
        }
        result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(json_metadata)
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row[3] == "my-schema"
        assert row["to_schema"] == "my-schema"

    def test_foreign_key_with_primary_key(self):
        """Test FOREIGN KEY parsing with mixed primary and foreign key constraints."""
        result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(MIXED_PK_FK_JSON)
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row[0] == "fk1"
        assert row["constraint_name"] == "fk1"
        assert row[1] == "ref_id"
        assert row["from_column"] == "ref_id"

    def test_no_foreign_key(self):
        """Test FOREIGN KEY parsing with no foreign key constraints."""
        json_metadata = {"table_constraints": "[(pk1,PRIMARY KEY (`id`))]"}
        result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(json_metadata)
        assert len(result.rows) == 0

    def test_no_table_constraints_field(self):
        """Test FOREIGN KEY parsing with no table_constraints field."""
        result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints({})
        assert len(result.rows) == 0

    def test_empty_string(self):
        """Test FOREIGN KEY parsing with an empty string."""
        result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(
            {"table_constraints": ""}
        )
        assert len(result.rows) == 0

    def test_spaces(self):
        """Test FOREIGN KEY parsing is robust to excessive spaces between keywords."""
        for num_extra_spaces in range(0, 40):
            es = " " * num_extra_spaces
            constraint_entry = (
                f"{es}({es}fk1{es},{es}FOREIGN {es}KEY{es}({es}`ref_id`{es})"
                f"{es}REFERENCES{es}`main`{es}.{es}`default`{es}.{es}`users`{es}"
                f"({es}`user_id`{es}){es}){es}"
            )
            json_metadata = {"table_constraints": f"[{constraint_entry}]"}
            result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(json_metadata)
            assert len(result.rows) == 1
            row = result.rows[0]
            assert row[0] == "fk1"
            assert row["constraint_name"] == "fk1"
            assert row[1] == "ref_id"
            assert row["from_column"] == "ref_id"
            assert row[2] == "main"
            assert row["to_catalog"] == "main"
            assert row[3] == "default"
            assert row["to_schema"] == "default"
            assert row[4] == "users"
            assert row["to_table"] == "users"
            assert row[5] == "user_id"
            assert row["to_column"] == "user_id"

    def test_many_constraints(self):
        """Test FOREIGN KEY parsing with many constraints in one string."""
        constraint_count = 20
        constraint_entries = [
            (
                f"(fk{i},FOREIGN KEY (`ref_col_{i}`)"
                f" REFERENCES `main`.`default`.`users_{i}` (`user_col_{i}`))"
            )
            for i in range(1, constraint_count + 1)
        ]
        json_metadata = {"table_constraints": f"[{', '.join(constraint_entries)}]"}
        result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(json_metadata)
        assert len(result.rows) == constraint_count
        for row_index in range(constraint_count):
            expected_constraint_name = f"fk{row_index + 1}"
            expected_from_column = f"ref_col_{row_index + 1}"
            expected_to_table = f"users_{row_index + 1}"
            expected_to_column = f"user_col_{row_index + 1}"
            row = result.rows[row_index]
            assert row[0] == expected_constraint_name
            assert row["constraint_name"] == expected_constraint_name
            assert row[1] == expected_from_column
            assert row["from_column"] == expected_from_column
            assert row[2] == "main"
            assert row["to_catalog"] == "main"
            assert row[3] == "default"
            assert row["to_schema"] == "default"
            assert row[4] == expected_to_table
            assert row["to_table"] == expected_to_table
            assert row[5] == expected_to_column
            assert row["to_column"] == expected_to_column

    def test_underscores_on_names(self):
        """Test FOREIGN KEY parsing with varying leading and trailing underscores."""
        for i in range(0, 20):
            underscores = "_" * i
            from_column = f"{underscores}ref_id{underscores}"
            to_catalog = f"{underscores}main{underscores}"
            to_schema = f"{underscores}default{underscores}"
            to_table = f"{underscores}users{underscores}"
            to_column = f"{underscores}user_id{underscores}"
            constraint_entry = (
                f"(fk1,FOREIGN KEY (`{from_column}`)"
                f" REFERENCES `{to_catalog}`.`{to_schema}`.`{to_table}` (`{to_column}`))"
            )

            json_metadata = {"table_constraints": f"[{constraint_entry}]"}
            result = DatabricksDescribeJsonMetadata.parse_foreign_key_constraints(json_metadata)
            assert len(result.rows) == 1
            row = result.rows[0]
            assert row[0] == "fk1"
            assert row["constraint_name"] == "fk1"
            assert row[1] == from_column
            assert row["from_column"] == from_column
            assert row[2] == to_catalog
            assert row["to_catalog"] == to_catalog
            assert row[3] == to_schema
            assert row["to_schema"] == to_schema
            assert row[4] == to_table
            assert row["to_table"] == to_table
            assert row[5] == to_column
            assert row["to_column"] == to_column


class TestParseNonNullConstraints:
    def test_mixed_nullable(self):
        """Test parsing of non-null constraints when some columns are nullable and some are not."""
        json_metadata = {
            "columns": [
                {"name": "id", "nullable": False},
                {"name": "email", "nullable": True},
            ]
        }
        result = DatabricksDescribeJsonMetadata.parse_non_null_constraints(json_metadata)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "id"
        assert result.rows[0]["column_name"] == "id"

    def test_all_nullable(self):
        """Test parsing of non-null constraints when all columns are nullable."""
        json_metadata = {
            "columns": [
                {"name": "a", "nullable": True},
                {"name": "b", "nullable": True},
            ]
        }
        result = DatabricksDescribeJsonMetadata.parse_non_null_constraints(json_metadata)
        assert len(result.rows) == 0

    def test_multiple_non_null(self):
        """Test parsing of non-null constraints when multiple columns are non-nullable."""
        json_metadata = {
            "columns": [
                {"name": "id", "nullable": False},
                {"name": "email", "nullable": False},
                {"name": "msg", "nullable": True},
            ]
        }
        result = DatabricksDescribeJsonMetadata.parse_non_null_constraints(json_metadata)
        assert len(result.rows) == 2
        assert result.rows[0][0] == "id"
        assert result.rows[0]["column_name"] == "id"
        assert result.rows[1][0] == "email"
        assert result.rows[1]["column_name"] == "email"

    def test_no_columns_key(self):
        """Test parsing of non-null constraints when there is no 'columns' key in the input."""
        result = DatabricksDescribeJsonMetadata.parse_non_null_constraints({})
        assert len(result.rows) == 0


class TestParseColumnMasks:
    def test_mask_with_using_columns(self):
        result = DatabricksDescribeJsonMetadata.parse_column_masks(COLUMN_MASK_JSON)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "phone_number"
        assert result.rows[0]["column_name"] == "phone_number"
        assert result.rows[0][1] == "main.db.mask_phone"
        assert result.rows[0]["mask_name"] == "main.db.mask_phone"
        assert result.rows[0][2] == "city"
        assert result.rows[0]["using_columns"] == "city"

    def test_mask_without_using_columns(self):
        json_metadata = {
            "column_masks": [
                {
                    "column_name": "ssn",
                    "function_name": {
                        "catalog_name": "main",
                        "schema_name": "db",
                        "function_name": "mask_ssn",
                    },
                    "using_column_names": [],
                }
            ]
        }
        result = DatabricksDescribeJsonMetadata.parse_column_masks(json_metadata)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "ssn"
        assert result.rows[0]["column_name"] == "ssn"
        assert result.rows[0][1] == "main.db.mask_ssn"
        assert result.rows[0]["mask_name"] == "main.db.mask_ssn"
        assert result.rows[0][2] is None
        assert result.rows[0]["using_columns"] is None

    def test_multiple_masks(self):
        json_metadata = {
            "column_masks": [
                {
                    "column_name": "col_a",
                    "function_name": {
                        "catalog_name": "c",
                        "schema_name": "s",
                        "function_name": "fn_a",
                    },
                    "using_column_names": ["x"],
                },
                {
                    "column_name": "col_b",
                    "function_name": {
                        "catalog_name": "c",
                        "schema_name": "s",
                        "function_name": "fn_b",
                    },
                    "using_column_names": [],
                },
            ]
        }
        result = DatabricksDescribeJsonMetadata.parse_column_masks(json_metadata)
        assert len(result.rows) == 2
        assert result.rows[0][0] == "col_a"
        assert result.rows[0]["column_name"] == "col_a"
        assert result.rows[0][1] == "c.s.fn_a"
        assert result.rows[0]["mask_name"] == "c.s.fn_a"
        assert result.rows[0][2] == "x"
        assert result.rows[0]["using_columns"] == "x"
        assert result.rows[1][0] == "col_b"
        assert result.rows[1]["column_name"] == "col_b"
        assert result.rows[1][1] == "c.s.fn_b"
        assert result.rows[1]["mask_name"] == "c.s.fn_b"
        assert result.rows[1][2] is None
        assert result.rows[1]["using_columns"] is None

    def test_no_column_masks_field(self):
        result = DatabricksDescribeJsonMetadata.parse_column_masks({})
        assert len(result.rows) == 0

    def test_empty_column_masks(self):
        result = DatabricksDescribeJsonMetadata.parse_column_masks({"column_masks": []})
        assert len(result.rows) == 0

    def test_mask_with_multiple_using_columns(self):
        json_input = {
            "column_masks": [
                {
                    "column_name": "secret",
                    "function_name": {
                        "catalog_name": "main",
                        "schema_name": "db",
                        "function_name": "mask_fn",
                    },
                    "using_column_names": ["col1", "col2", "col3"],
                }
            ]
        }
        result = DatabricksDescribeJsonMetadata.parse_column_masks(json_input)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "secret"
        assert result.rows[0]["column_name"] == "secret"
        assert result.rows[0][1] == "main.db.mask_fn"
        assert result.rows[0]["mask_name"] == "main.db.mask_fn"
        assert result.rows[0][2] == "col1,col2,col3"
        assert result.rows[0]["using_columns"] == "col1,col2,col3"

    def test_mask_missing_using_column_names_key(self):
        json_input = {
            "column_masks": [
                {
                    "column_name": "secret",
                    "function_name": {
                        "catalog_name": "main",
                        "schema_name": "db",
                        "function_name": "mask_fn",
                    },
                }
            ]
        }
        result = DatabricksDescribeJsonMetadata.parse_column_masks(json_input)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "secret"
        assert result.rows[0]["column_name"] == "secret"
        assert result.rows[0][1] == "main.db.mask_fn"
        assert result.rows[0]["mask_name"] == "main.db.mask_fn"
        assert result.rows[0][2] is None
        assert result.rows[0]["using_columns"] is None


class TestParseRowFilter:
    def test_row_filter_with_single_target_column(self):
        result = DatabricksDescribeJsonMetadata.parse_row_filter(ROW_FILTER_JSON)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "default_catalog"
        assert result.rows[0]["table_catalog"] == "default_catalog"
        assert result.rows[0][1] == "default"
        assert result.rows[0]["table_schema"] == "default"
        assert result.rows[0][2] == "table_with_row_filter"
        assert result.rows[0]["table_name"] == "table_with_row_filter"
        assert result.rows[0][3] == "default_catalog.default.filter_by_region"
        assert result.rows[0]["filter_name"] == "default_catalog.default.filter_by_region"
        assert result.rows[0][4] == "region"
        assert result.rows[0]["target_columns"] == "region"

    def test_row_filter_with_multiple_target_columns(self):
        result = DatabricksDescribeJsonMetadata.parse_row_filter(ROW_FILTER_MULTI_COLUMN_JSON)
        assert len(result.rows) == 1
        assert result.rows[0][0] == "default_catalog"
        assert result.rows[0]["table_catalog"] == "default_catalog"
        assert result.rows[0][1] == "default"
        assert result.rows[0]["table_schema"] == "default"
        assert result.rows[0][2] == "table_with_row_filter"
        assert result.rows[0]["table_name"] == "table_with_row_filter"
        assert result.rows[0][3] == "default_catalog.default.filter_by_dept_and_region"
        assert result.rows[0]["filter_name"] == "default_catalog.default.filter_by_dept_and_region"
        assert result.rows[0][4] == "department,region"
        assert result.rows[0]["target_columns"] == "department,region"

    def test_no_row_filter_field(self):
        result = DatabricksDescribeJsonMetadata.parse_row_filter(PLAIN_TABLE_JSON)
        assert len(result.rows) == 0


class TestParseViewDescription:
    def test_with_view_text(self):
        json_metadata = {"view_text": "SELECT id, name FROM main.default.source_table"}
        result = DatabricksDescribeJsonMetadata.parse_view_description(json_metadata)
        assert result["view_definition"] == "SELECT id, name FROM main.default.source_table"

    def test_without_view_text(self):
        json_metadata = {
            "columns": [
                {"name": "id", "nullable": True},
                {"name": "value", "nullable": True},
            ],
        }
        result = DatabricksDescribeJsonMetadata.parse_view_description(json_metadata)
        assert len(result.values()) == 0

    def test_null_view_text(self):
        result = DatabricksDescribeJsonMetadata.parse_view_description({"view_text": None})
        assert len(result.values()) == 0


class TestFromJsonMetadata:
    def test_table_with_column_masks(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COLUMN_MASK_JSON)
        assert len(metadata.column_masks.rows) == 1
        assert metadata.column_masks.rows[0][0] == "phone_number"
        assert metadata.column_masks.rows[0]["column_name"] == "phone_number"
        assert metadata.column_masks.rows[0][1] == "main.db.mask_phone"
        assert metadata.column_masks.rows[0]["mask_name"] == "main.db.mask_phone"
        assert metadata.column_masks.rows[0][2] == "city"
        assert metadata.column_masks.rows[0]["using_columns"] == "city"
        assert len(metadata.primary_key_constraints.rows) == 0
        assert len(metadata.foreign_key_constraints.rows) == 0

    def test_table_with_row_filter(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ROW_FILTER_JSON)
        assert len(metadata.row_filters.rows) == 1
        assert metadata.row_filters.rows[0][0] == "default_catalog"
        assert metadata.row_filters.rows[0]["table_catalog"] == "default_catalog"
        assert metadata.row_filters.rows[0][1] == "default"
        assert metadata.row_filters.rows[0]["table_schema"] == "default"
        assert metadata.row_filters.rows[0][2] == "table_with_row_filter"
        assert metadata.row_filters.rows[0]["table_name"] == "table_with_row_filter"
        assert metadata.row_filters.rows[0][3] == "default_catalog.default.filter_by_region"
        assert (
            metadata.row_filters.rows[0]["filter_name"]
            == "default_catalog.default.filter_by_region"
        )
        assert metadata.row_filters.rows[0][4] == "region"
        assert metadata.row_filters.rows[0]["target_columns"] == "region"
        assert len(metadata.primary_key_constraints.rows) == 0
        assert len(metadata.foreign_key_constraints.rows) == 0
        assert len(metadata.column_masks.rows) == 0

    def test_materialized_view(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(MATERIALIZED_VIEW_JSON)
        assert metadata.view_description["view_definition"] == (
            "SELECT id, name FROM main.default.source_table"
        )
        assert len(metadata.primary_key_constraints.rows) == 0
        assert len(metadata.column_masks.rows) == 0

    def test_all_fields_populated(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ALL_FIELDS_JSON)
        # PK
        assert len(metadata.primary_key_constraints.rows) == 1
        assert metadata.primary_key_constraints.rows[0]["constraint_name"] == "pk1"
        assert metadata.primary_key_constraints.rows[0]["column_name"] == "id"
        # FK
        assert len(metadata.foreign_key_constraints.rows) == 1
        fk = metadata.foreign_key_constraints.rows[0]
        assert fk["constraint_name"] == "fk1"
        assert fk["from_column"] == "id"
        assert fk["to_catalog"] == "main"
        assert fk["to_schema"] == "default"
        assert fk["to_table"] == "other"
        assert fk["to_column"] == "other_id"
        # Non-null
        assert len(metadata.non_null_constraints.rows) == 1
        assert metadata.non_null_constraints.rows[0]["column_name"] == "id"
        # Column masks
        assert len(metadata.column_masks.rows) == 1
        assert metadata.column_masks.rows[0]["column_name"] == "secret"
        assert metadata.column_masks.rows[0]["mask_name"] == "main.db.mask_secret"
        assert metadata.column_masks.rows[0]["using_columns"] == "id"
        # Row filters
        assert len(metadata.row_filters.rows) == 1
        assert metadata.row_filters.rows[0]["table_catalog"] == "main"
        assert metadata.row_filters.rows[0]["table_schema"] == "default"
        assert metadata.row_filters.rows[0]["table_name"] == "source"
        assert metadata.row_filters.rows[0]["filter_name"] == "main.db.filter_secret"
        assert metadata.row_filters.rows[0]["target_columns"] == "id"
        # View description
        assert metadata.view_description["view_definition"] == (
            "SELECT id, secret FROM main.default.source"
        )

    def test_plain_table(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(PLAIN_TABLE_JSON)
        assert len(metadata.primary_key_constraints.rows) == 0
        assert len(metadata.foreign_key_constraints.rows) == 0
        assert len(metadata.non_null_constraints.rows) == 0
        assert len(metadata.row_filters.rows) == 0
        assert len(metadata.column_masks.rows) == 0
        assert len(metadata.view_description.values()) == 0


class TestParserToConstraintsProcessor:
    @staticmethod
    def _build_results(metadata):
        return {
            "non_null_constraint_columns": metadata.non_null_constraints,
            "primary_key_constraints": metadata.primary_key_constraints,
            "foreign_key_constraints": metadata.foreign_key_constraints,
        }

    def test_single_pk_roundtrip(self):
        json_metadata = {
            "columns": [{"name": "id", "nullable": False}],
            "table_constraints": "[(pk1,PRIMARY KEY (`id`))]",
        }
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(json_metadata)
        config = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        assert config == ConstraintsConfig(
            set_non_nulls={"id"},
            set_constraints={
                PrimaryKeyConstraint(type=ConstraintType.primary_key, name="pk1", columns=["id"]),
            },
        )

    def test_composite_pk_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COMPOSITE_PK_JSON)
        config = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        assert config == ConstraintsConfig(
            set_non_nulls={"id", "name"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="id_name_pk",
                    columns=["id", "name"],
                )
            },
        )

    def test_single_fk_roundtrip(self):
        json_metadata = {
            "columns": [{"name": "ref_id", "nullable": True}],
            "table_constraints": (
                "[(fk1,FOREIGN KEY (`ref_id`) REFERENCES `main`.`default`.`other` (`other_id`))]"
            ),
        }
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(json_metadata)
        config = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        assert config == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk1",
                    columns=["ref_id"],
                    to="`main`.`default`.`other`",
                    to_columns=["other_id"],
                )
            },
        )

    def test_composite_fk_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COMPOSITE_FK_JSON)
        config = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        assert config == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="fk_pk",
                    columns=["id"],
                ),
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="child_fk",
                    columns=["ref_id", "ref_name"],
                    to="`main`.`default`.`parents`",
                    to_columns=["id", "name"],
                ),
            },
        )

    def test_mixed_constraints_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(EMAIL_ADDRESSES_JSON)
        config = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        assert config.set_non_nulls == {"address_id"}
        assert any(
            isinstance(c, PrimaryKeyConstraint) and c.name == "email_ad_pk"
            for c in config.set_constraints
        )
        assert any(
            isinstance(c, ForeignKeyConstraint)
            and c.name == "email_fk"
            and c.to == "`main`.`default`.`users`"
            for c in config.set_constraints
        )

    def test_no_constraints_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(PLAIN_TABLE_JSON)
        config = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        assert config == ConstraintsConfig(set_non_nulls=set(), set_constraints=set())


class TestParserToColumnMaskProcessor:
    def test_mask_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COLUMN_MASK_JSON)
        config = ColumnMaskProcessor.from_relation_results({"column_masks": metadata.column_masks})
        assert config == ColumnMaskConfig(
            set_column_masks={
                "phone_number": {
                    "function": "main.db.mask_phone",
                    "using_columns": "city",
                }
            }
        )

    def test_no_masks_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(PLAIN_TABLE_JSON)
        config = ColumnMaskProcessor.from_relation_results({"column_masks": metadata.column_masks})
        assert config == ColumnMaskConfig(set_column_masks={})

    def test_mask_no_false_diff(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COLUMN_MASK_JSON)
        existing = ColumnMaskProcessor.from_relation_results(
            {"column_masks": metadata.column_masks}
        )
        model = ColumnMaskConfig(
            set_column_masks={
                "phone_number": {
                    "function": "main.db.mask_phone",
                    "using_columns": "city",
                }
            }
        )
        assert model.get_diff(existing) is None

    def test_mask_diff_change_function(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COLUMN_MASK_JSON)
        existing = ColumnMaskProcessor.from_relation_results(
            {"column_masks": metadata.column_masks}
        )
        model = ColumnMaskConfig(
            set_column_masks={
                "phone_number": {
                    "function": "main.db.new_mask_fn",
                    "using_columns": "city",
                }
            }
        )
        diff = model.get_diff(existing)
        assert diff is not None
        assert diff.set_column_masks == {
            "phone_number": {
                "function": "main.db.new_mask_fn",
                "using_columns": "city",
            }
        }

    def test_mask_diff_add_new_mask(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COLUMN_MASK_JSON)
        existing = ColumnMaskProcessor.from_relation_results(
            {"column_masks": metadata.column_masks}
        )
        model = ColumnMaskConfig(
            set_column_masks={
                "phone_number": {
                    "function": "main.db.mask_phone",
                    "using_columns": "city",
                },
                "ssn": {
                    "function": "main.db.mask_ssn",
                },
            }
        )
        diff = model.get_diff(existing)
        assert diff is not None
        assert "ssn" in diff.set_column_masks
        assert "phone_number" not in diff.set_column_masks


class TestParserToRowFilterProcessor:
    def test_row_filter_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ROW_FILTER_JSON)
        config = RowFilterProcessor.from_relation_results({"row_filters": metadata.row_filters})
        assert config == RowFilterConfig(
            function="default_catalog.default.filter_by_region",
            columns=("region",),
        )

    def test_multi_column_row_filter_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ROW_FILTER_MULTI_COLUMN_JSON)
        config = RowFilterProcessor.from_relation_results({"row_filters": metadata.row_filters})
        assert config == RowFilterConfig(
            function="default_catalog.default.filter_by_dept_and_region",
            columns=("department", "region"),
        )

    def test_no_row_filter_roundtrip(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(PLAIN_TABLE_JSON)
        config = RowFilterProcessor.from_relation_results({"row_filters": metadata.row_filters})
        assert config == RowFilterConfig()

    def test_row_filter_no_false_diff(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ROW_FILTER_JSON)
        existing = RowFilterProcessor.from_relation_results({"row_filters": metadata.row_filters})
        model = RowFilterConfig(
            function="default_catalog.default.filter_by_region",
            columns=("region",),
        )
        assert model.get_diff(existing) is None

    def test_row_filter_diff_change_function(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ROW_FILTER_JSON)
        existing = RowFilterProcessor.from_relation_results({"row_filters": metadata.row_filters})
        model = RowFilterConfig(
            function="default_catalog.default.filter_by_department",
            columns=("region",),
        )
        diff = model.get_diff(existing)
        assert diff is not None
        assert diff == RowFilterConfig(
            function="default_catalog.default.filter_by_department",
            columns=("region",),
            is_change=True,
        )

    def test_row_filter_diff_change_columns(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ROW_FILTER_JSON)
        existing = RowFilterProcessor.from_relation_results({"row_filters": metadata.row_filters})
        model = RowFilterConfig(
            function="default_catalog.default.filter_by_region",
            columns=("department", "region"),
        )
        diff = model.get_diff(existing)
        assert diff is not None
        assert diff == RowFilterConfig(
            function="default_catalog.default.filter_by_region",
            columns=("department", "region"),
            is_change=True,
        )

    def test_row_filter_diff_unset(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(ROW_FILTER_JSON)
        existing = RowFilterProcessor.from_relation_results({"row_filters": metadata.row_filters})
        model = RowFilterConfig()
        diff = model.get_diff(existing)
        assert diff is not None
        assert diff == RowFilterConfig(should_unset=True, is_change=True)


class TestParserToQueryProcessor:
    def test_mv_view_text_roundtrip(self):
        view_desc = DatabricksDescribeJsonMetadata.parse_view_description(MATERIALIZED_VIEW_JSON)
        config = QueryProcessor.from_relation_results({"information_schema.views": view_desc})
        assert config == QueryConfig(query="SELECT id, name FROM main.default.source_table")

    def test_view_text_roundtrip(self):
        view_desc = DatabricksDescribeJsonMetadata.parse_view_description(REGULAR_VIEW_JSON)
        config = QueryProcessor.from_relation_results({"information_schema.views": view_desc})
        assert config == QueryConfig(query="SELECT id, name FROM main.default.other_table")

    def test_view_text_with_outer_parens(self):
        view_desc = DatabricksDescribeJsonMetadata.parse_view_description(
            {"view_text": "(SELECT id FROM t)"}
        )
        config = QueryProcessor.from_relation_results({"information_schema.views": view_desc})
        assert config == QueryConfig(query="SELECT id FROM t")


class TestParserToQueryDiff:
    def test_no_false_diff_on_identical_query(self):
        view_desc = DatabricksDescribeJsonMetadata.parse_view_description(MATERIALIZED_VIEW_JSON)
        existing = QueryProcessor.from_relation_results({"information_schema.views": view_desc})
        model = QueryConfig(query="SELECT id, name FROM main.default.source_table")
        assert model.get_diff(existing) is None

    def test_detects_real_query_change(self):
        view_desc = DatabricksDescribeJsonMetadata.parse_view_description(MATERIALIZED_VIEW_JSON)
        existing = QueryProcessor.from_relation_results({"information_schema.views": view_desc})
        model = QueryConfig(query="SELECT id FROM different_table")
        diff = model.get_diff(existing)
        assert diff is not None
        assert diff.query == "SELECT id FROM different_table"


class TestParserToConstraintsDiff:
    @staticmethod
    def _build_results(metadata):
        return {
            "non_null_constraint_columns": metadata.non_null_constraints,
            "primary_key_constraints": metadata.primary_key_constraints,
            "foreign_key_constraints": metadata.foreign_key_constraints,
        }

    def test_composite_pk_no_false_diff(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COMPOSITE_PK_JSON)
        existing = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        model = ConstraintsConfig(
            set_non_nulls={"id", "name"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="id_name_pk",
                    columns=["id", "name"],
                )
            },
        )
        assert model.get_diff(existing) is None

    def test_composite_pk_diff_add_column(self):
        """Model adds a column to PK — diff should set new PK, unset old PK, set new non-null."""
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COMPOSITE_PK_JSON)
        existing = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        model = ConstraintsConfig(
            set_non_nulls={"id", "name", "value"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="new_pk",
                    columns=["id", "name", "value"],
                )
            },
        )
        diff = model.get_diff(existing)
        assert diff is not None
        assert diff.set_non_nulls == {"value"}
        assert diff.unset_non_nulls == set()
        assert len(diff.unset_constraints) == 1
        unset = next(iter(diff.unset_constraints))
        assert isinstance(unset, PrimaryKeyConstraint)
        assert unset.name == "id_name_pk"
        assert unset.columns == ["id", "name"]
        assert len(diff.set_constraints) == 1
        added = next(iter(diff.set_constraints))
        assert isinstance(added, PrimaryKeyConstraint)
        assert added.name == "new_pk"
        assert added.columns == ["id", "name", "value"]

    def test_composite_fk_diff_change_target(self):
        """Model changes FK target — diff should unset old FK, set new FK."""
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COMPOSITE_FK_JSON)
        existing = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        model = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="fk_pk",
                    columns=["id"],
                ),
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="new_fk",
                    columns=["ref_id"],
                    to="`main`.`default`.`other_table`",
                    to_columns=["other_id"],
                ),
            },
        )
        diff = model.get_diff(existing)
        assert diff is not None
        assert diff.set_non_nulls == set()
        assert diff.unset_non_nulls == set()
        # Old FK unset
        unset_fks = {c for c in diff.unset_constraints if isinstance(c, ForeignKeyConstraint)}
        assert len(unset_fks) == 1
        unset_fk = next(iter(unset_fks))
        assert unset_fk.name == "child_fk"
        assert unset_fk.columns == ["ref_id", "ref_name"]
        assert unset_fk.to == "`main`.`default`.`parents`"
        # New FK set
        set_fks = {c for c in diff.set_constraints if isinstance(c, ForeignKeyConstraint)}
        assert len(set_fks) == 1
        set_fk = next(iter(set_fks))
        assert set_fk.name == "new_fk"
        assert set_fk.columns == ["ref_id"]
        assert set_fk.to == "`main`.`default`.`other_table`"
        assert set_fk.to_columns == ["other_id"]

    def test_composite_fk_no_false_diff(self):
        metadata = DatabricksDescribeJsonMetadata.from_json_metadata(COMPOSITE_FK_JSON)
        existing = ConstraintsProcessor.from_relation_results(self._build_results(metadata))
        model = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="fk_pk",
                    columns=["id"],
                ),
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="child_fk",
                    columns=["ref_id", "ref_name"],
                    to="`main`.`default`.`parents`",
                    to_columns=["id", "name"],
                ),
            },
        )
        assert model.get_diff(existing) is None
