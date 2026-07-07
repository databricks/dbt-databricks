# split_part boundary cases

# part_number 0 resolves to the first element; an index past the end of the array
# (positive or negative) resolves to NULL rather than raising, because the macro
# extracts with get(...) instead of an array subscript.
models__test_split_part_boundaries_sql = """
select {{ split_part("'a|b|c'", "'|'", 0) }} as actual, 'a' as expected
union all
select {{ split_part("'1|2|3'", "'|'", 0) }} as actual, '1' as expected
union all
select {{ split_part("'a|b|c'", "'|'", 5) }} as actual, cast(null as string) as expected
union all
select {{ split_part("'a|b|c'", "'|'", -5) }} as actual, cast(null as string) as expected
"""


models__test_split_part_boundaries_yml = """
version: 2
models:
  - name: test_split_part_boundaries
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
