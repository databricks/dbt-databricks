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
