create_table_statement = """
create table {database}.`{schema}`.`on_model_hook` (
    test_state       STRING, -- start|end
    target_dbname    STRING,
    target_host      STRING,
    target_name      STRING,
    target_schema    STRING,
    target_type      STRING,
    target_user      STRING,
    target_pass      STRING,
    target_threads   INT,
    run_started_at   STRING,
    invocation_id    STRING,
    thread_id        STRING
)
"""

MODEL_PRE_HOOK = """
   insert into `{{this.database}}`.`{{this.schema}}`.`on_model_hook` (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    'start',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )
"""

MODEL_POST_HOOK = """
   insert into `{{this.database}}`.`{{this.schema}}`.`on_model_hook` (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    'end',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )
"""
