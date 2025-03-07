basic_seed_csv = """
id,msg
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
select cast(2 as bigint) as id, 'goodbye' as msg
"""


class AnyStringOrNone:
    """Any string. Use this in assert calls"""

    def __eq__(self, other):
        return isinstance(other, str) or other is None
