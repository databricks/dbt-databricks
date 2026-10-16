upstream_sql = """
select 1 as id
"""

terminal_semicolon_model_sql = """
select id * 10 as id from {{ ref('upstream') }};
"""

terminal_semicolon_unit_test_yml = """
unit_tests:
  - name: test_terminal_semicolon_model
    model: terminal_semicolon_model
    given:
      - input: ref('upstream')
        rows:
          - {id: 1}
          - {id: 2}
    expect:
      rows:
        - {id: 10}
        - {id: 20}
"""
