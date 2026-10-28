my_upstream_model_sql = """
select 1 as id
"""

my_incr_model_sql = """
{{ config(materialized='incremental') }}

select u.id, s.val
from {{ ref('my-upstream-model') }} as u
join {{ source('my_src', 'my-source-table') }} as s on u.id = s.id
{% if is_incremental() %}
where u.id not in (select id from {{ this }})
{% endif %}
"""

hyphenated_names_yml = """
version: 2

sources:
  - name: my_src
    tables:
      - name: my-source-table

unit_tests:
  - name: test_hyphenated_ref_and_source
    model: my-incr-model
    overrides:
      macros:
        is_incremental: false
    given:
      - input: ref('my-upstream-model')
        format: sql
        rows: select 1 as id union all select 2 as id
      - input: source('my_src', 'my-source-table')
        format: sql
        rows: select 1 as id, 'a' as val union all select 2 as id, 'b' as val
    expect:
      rows:
        - {id: 1, val: a}
        - {id: 2, val: b}

  - name: test_hyphenated_this
    model: my-incr-model
    overrides:
      macros:
        is_incremental: true
    given:
      - input: ref('my-upstream-model')
        format: sql
        rows: select 1 as id union all select 2 as id
      - input: source('my_src', 'my-source-table')
        format: sql
        rows: select 1 as id, 'a' as val union all select 2 as id, 'b' as val
      - input: this
        format: sql
        rows: select 1 as id
    expect:
      rows:
        - {id: 2, val: b}
"""
