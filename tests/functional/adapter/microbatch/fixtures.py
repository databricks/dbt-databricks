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
