basic_seed_csv = """id,msg
1,hello
2,goodbye
2,yo
3,anyway
"""

basic_model_sql = """
{{ config(
    materialized = 'table'
)}}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg;
"""


class AnyStringOrNone:
    """Any string. Use this in assert calls"""

    def __eq__(self, other):
        return isinstance(other, str) or other is None


all_types_model_sql = """
{{ config(
    materialized = 'table'
)}}

SELECT
  -- Numeric types
  CAST(42 AS TINYINT) AS tinyint_col,
  CAST(30000 AS SMALLINT) AS smallint_col,
  CAST(2000000000 AS INT) AS int_col,
  CAST(9000000000000000000 AS BIGINT) AS bigint_col,
  CAST(3.14 AS FLOAT) AS float_col,
  CAST(2.718281828459 AS DOUBLE) AS double_col,
  CAST(12345.6789 AS DECIMAL(10,4)) AS decimal_col,

  -- Boolean type
  CAST(TRUE AS BOOLEAN) AS boolean_col,

  -- String types
  CAST('Hello' AS STRING) AS string_col,
  CAST('Databricks' AS VARCHAR(50)) AS varchar_col,
  CAST('abc' AS CHAR(10)) AS char_col,

  -- Binary type
  CAST(X'DEADBEEF' AS BINARY) AS binary_col,

  -- Date and time types
  CAST('2024-07-08' AS DATE) AS date_col,
  CAST('2024-07-08 12:34:56' AS TIMESTAMP) AS timestamp_col,

  -- Variant type
  CAST(1 AS VARIANT) AS variant_col,

  -- Complex types: Array, Map, Struct
  CAST(ARRAY(CAST(1 AS INT), CAST(2 AS INT), CAST(3 AS INT)) AS ARRAY<INT>) AS array_int_col,

  CAST(MAP(CAST('key1' AS STRING), CAST('value1' AS STRING), CAST('key2' AS STRING),
    CAST('value2' AS STRING)) AS MAP<STRING,STRING>) AS map_col,

  -- Nested STRUCT up to depth 3
  CAST(NAMED_STRUCT(
    'level1_field', CAST(1 AS INT),
    'level1_struct', CAST(NAMED_STRUCT(
      'level2_field', CAST('abc' AS STRING),
      'level2_array', CAST(ARRAY(
        CAST(NAMED_STRUCT(
          'level3_field', CAST(TRUE AS BOOLEAN)
        ) AS STRUCT<level3_field: BOOLEAN>)
      ) AS ARRAY<STRUCT<level3_field: BOOLEAN>>)
    ) AS STRUCT<level2_field: STRING, level2_array: ARRAY<STRUCT<level3_field: BOOLEAN>>>)
  ) AS STRUCT<
    level1_field: INT,
    level1_struct: STRUCT<
      level2_field: STRING,
      level2_array: ARRAY<STRUCT<level3_field: BOOLEAN>>
    >
  >) AS complex_struct
"""

# Create a flat struct with 30 fields to test cases where DESCRIBE TABLE returns truncated type info
pairs = []
for i in range(0, 30):
    pairs.append(f"'field{i}', CAST({i} AS INT)")

# Join the pairs with commas
pairs_str = ",".join(pairs)

# Wrap in SELECT named_struct
big_struct_model_sql = f"""
{{{{ config(
    materialized = 'table'
)}}}}

SELECT named_struct(
  {pairs_str}
) AS big_struct;
"""
