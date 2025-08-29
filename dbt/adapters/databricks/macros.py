"""
Macro name constants for the Databricks adapter.

This module contains all macro names used throughout the adapter to ensure
consistency and make them easy to update.
"""

# Catalog-related macros
CURRENT_CATALOG_MACRO_NAME = "current_catalog"
USE_CATALOG_MACRO_NAME = "use_catalog"
GET_CATALOG_MACRO_NAME = "get_catalog"

# Table/view information macros
SHOW_TABLE_EXTENDED_MACRO_NAME = "show_table_extended"
SHOW_TABLES_MACRO_NAME = "show_tables"
SHOW_VIEWS_MACRO_NAME = "show_views"

DESCRIBE_TABLE_EXTENDED_MACRO_NAME = "describe_table_extended_without_caching"
GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME = "get_columns_in_relation_raw"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"
