## Databricks Workflow Job Submission

Use the `workflow_job` submission method to run your python model as a long-lived
Databricks Workflow. Models look the same as they would using the `job_cluster` submission
method, but allow for additional configuration.

Some of that configuration can also be used for `job_cluster` models.

```python
# my_model.py
import pyspark.sql.types as T
import pyspark.sql.functions as F


def model(dbt, session):
    dbt.config(
        materialized='incremental',
        submission_method='workflow_job'
    )

    output_schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
    ])
    return spark.createDataFrame(data=spark.sparkContext.emptyRDD(), schema=output_schema)
```

The config for a model could look like:

```yaml
models:
  - name: my_model
    config:
      python_job_config:
        # This is also applied to one-time run models
        email_notifications: { on_failure: ["reynoldxin@databricks.com"] }
        max_retries: 2
        timeout_seconds: 18000
        existing_cluster_id: 1234a-123-1234 # Use in place of job_cluster_config or null

        # Name must be unique unless existing_job_id is also defined
        name: my_workflow_name
        existing_job_id: 12341234

        # Override settings for your model's dbt task. For instance, you can
        # change the task key
        additional_task_settings: { "task_key": "my_dbt_task" }

        # Define tasks to run before/after the model
        post_hook_tasks:
          [
            {
              "depends_on": [{ "task_key": "my_dbt_task" }],
              "task_key": "OPTIMIZE_AND_VACUUM",
              "notebook_task":
                { "notebook_path": "/my_notebook_path", "source": "WORKSPACE" },
            },
          ]

        # Also applied to one-time run models
        grants:
          view: [{ "group_name": "marketing-team" }]
          run: [{ "user_name": "alighodsi@databricks.com" }]
          manage: []

      # Reused for the workflow job cluster definition
      job_cluster_config:
        spark_version: "15.3.x-scala2.12"
        node_type_id: "rd-fleet.2xlarge"
        runtime_engine: "{{ var('job_cluster_defaults.runtime_engine') }}"
        data_security_mode: "{{ var('job_cluster_defaults.data_security_mode') }}"
        autoscale: { "min_workers": 1, "max_workers": 4 }
```

### Configuration

All config values are optional. See the Databricks Jobs API for the full list of attributes
that can be set.

#### Reuse in job_cluster submission method

If the following values are defined in `config.python_job_config`, they will be used even if
the model uses the job_cluster submission method. For example, you can define a job_cluster model
to send an email notification on failure.

- grants
- email_notifications
- webhook_notifications
- notification_settings
- timeout_seconds
- health
- environments

#### Workflow name

The name of the workflow must be unique unless you also define an existing job id. By default,
dbt will generate a name based on the catalog, schema, and model identifier.

#### Clusters

- If defined, dbt will re-use the `config.job_cluster_config` to define a job cluster for the workflow tasks.
- If `config.python_job_config.existing_cluster_id` is defined, dbt will use that cluster
- Similarly, you can define a reusable job cluster for the workflow and tell the task to use that
- If none of those are in the configuration, the task cluster will be serverless

```yaml
# Reusable job cluster config example

models:
  - name: my_model

    config:
      python_job_config:
        additional_task_settings:
          { task_key: "task_a", job_cluster_key: "cluster_a" }
        post_hook_tasks:
          [
            {
              depends_on: [{ "task_key": "task_a" }],
              task_key: "OPTIMIZE_AND_VACUUM",
              job_cluster_key: "cluster_a",
              notebook_task:
                {
                  notebook_path: "/OPTIMIZE_AND_VACUUM",
                  source: "WORKSPACE",
                  base_parameters:
                    {
                      database: "{{ target.database }}",
                      schema: "{{ target.schema }}",
                      table_name: "my_model",
                    },
                },
            },
          ]
        job_clusters:
          [
            {
              job_cluster_key: "cluster_a",
              new_cluster:
                {
                  spark_version: "{{ var('dbr_versions')['lts_v14'] }}",
                  node_type_id: "{{ var('cluster_node_types')['large_job'] }}",
                  runtime_engine: "{{ var('job_cluster_defaults.runtime_engine') }}",
                  autoscale: { "min_workers": 1, "max_workers": 2 },
                },
            },
          ]
```

#### Grants

You might want to give certain users or teams access to run your workflows outside of
dbt in an ad hoc way. You can define those permissions in the `python_job_config.grants`.
The owner will always be the user or service principal creating the workflows.

These grants will also be applied to one-time run models using the `job_cluster` submission
method.

The dbt rules correspond with the following Databricks permissions:

- view: `CAN_VIEW`
- run: `CAN_MANAGE_RUN`
- manage: `CAN_MANAGE`

```
grants:
  view: [
    {"group_name": "marketing-team"},
  ]
  run: [
    {"user_name": "alighodsi@databricks.com"}
  ]
  manage: []
```

#### Post hooks

It is possible to add in python hooks by using the `config.python_job_config.post_hook_tasks`
attribute. You will need to define the cluster for each task, or use a reusable one from
`config.python_job_config.job_clusters`.
