# dateadd

seeds__data_dateadd_csv = """from_time,interval_length,datepart,result
2018-01-01 01:00:00,1,day,2018-01-02 01:00:00
2018-01-01 01:00:00,1,week,2018-01-08 01:00:00
2018-01-01 01:00:00,1,month,2018-02-01 01:00:00
2018-01-01 01:00:00,1,quarter,2018-04-01 01:00:00
2018-01-01 01:00:00,1,year,2019-01-01 01:00:00
2018-01-01 01:00:00,1,hour,2018-01-01 02:00:00
2018-01-01 01:00:00,1,minute,2018-01-01 01:01:00
2018-01-01 01:00:00,1,second,2018-01-01 01:00:01
,1,day,
"""


models__test_dateadd_sql = """
with data as (

    select * from {{ ref('data_dateadd') }}

)

select
    case
        when datepart = 'day' then cast(
            {{ dateadd('day', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        when datepart = 'week' then cast(
            {{ dateadd('week', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        when datepart = 'month' then cast(
            {{ dateadd('month', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        when datepart = 'quarter' then cast(
            {{ dateadd('quarter', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        when datepart = 'year' then cast(
            {{ dateadd('year', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        when datepart = 'hour' then cast(
            {{ dateadd('hour', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        when datepart = 'minute' then cast(
            {{ dateadd('minute', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        when datepart = 'second' then cast(
            {{ dateadd('second', 'interval_length', 'from_time') }}
            as {{ api.Column.translate_type('timestamp') }}
        )
        else null
    end as actual,
    result as expected

from data
"""
