schema = """version: 2
models:
  - name: input_model

  - name: microbatch_model
    config:
      persist_docs:
        relation: True
        columns: True
    description: This is a microbatch model
    columns:
      - name: id
        description: "Id of the model"
      - name: event_time
        description: "Timestamp of the event"
"""

microbatch_seeds_csv = """
id,event_time,amount
1,2023-01-01,100
2,2023-01-01,200
3,2023-01-02,300
""".strip()

# Initial model: columns in (id, event_time, amount) order
microbatch_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    begin='2023-01-01',
    batch_size='day'
) }}
select id, event_time, amount from {{ ref('microbatch_seeds') }}
"""

# Reordered model: columns in (amount, id, event_time) order — this is the key scenario
# Without BY NAME, positional INSERT would silently corrupt data here
microbatch_model_reordered_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    begin='2023-01-01',
    batch_size='day'
) }}
select amount, id, event_time from {{ ref('microbatch_seeds') }}
"""

schema_yml = """
version: 2
models:
  - name: microbatch_model
    columns:
      - name: id
      - name: event_time
      - name: amount
"""

# Input spanning five days so a subsequent run produces multiple batches (first + middle +
# last), exercising the concurrent path where middle batches run in parallel.
concurrent_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00-0' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00-0' as event_time
union all
select 4 as id, TIMESTAMP '2020-01-04 00:00:00-0' as event_time
union all
select 5 as id, TIMESTAMP '2020-01-05 00:00:00-0' as event_time
"""

# Microbatch model carrying both config-change triggers from issue #1443: liquid_clustered_by
# (emits ALTER ... CLUSTER BY) and tblproperties (emits ALTER ... SET TBLPROPERTIES). Under
# concurrent batches these must run on the first batch only, or they collide with the other
# batches' writes and fail them.
concurrent_microbatch_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    concurrent_batches=true,
    liquid_clustered_by=['id'],
    tblproperties={'delta.columnMapping.mode': 'name'}
) }}
select * from {{ ref('concurrent_input_model') }}
"""
