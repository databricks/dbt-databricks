# Local development with dbt-databricks
This page describes how to develop a dbt project on your computer using `dbt-databricks`. We will create an empty dbt project with information on how to connect to Databricks. We will then run our first dbt models.

## Prerequisites
- Access to a Databricks workspace
- Ability to create a Personal Access Token (PAT)
- Python 3.8+
- dbt-core v1.1.0+
- dbt-databricks v1.1.0+

##  Prepare to connect
### Collect connection information
Before you scaffold a new dbt project, you have to collect some information which dbt will use to connect to Databricks. Where you find this information depends on whether you are using Databricks Clusters or Databricks SQL endpoints. We recommend that you develop dbt models against Databricks SQL endpoints as they provide the latest SQL features and optimizations.

#### Databricks SQL Warehouses
1. Log in to your Databricks workspace 
2. Click the _SQL_ persona in the left navigation bar to switch to Databricks SQL
3. Click _SQL Endpoints_
4. Choose the SQL endpoint you want to connect to
5. Click _Connection details_
6. Copy the value of _Server hostname_. This will be the value of `host` when you scaffold a dbt project.
7. Copy the value of _HTTP path_.  This will be the value of `http_path` when you scaffold a dbt project.

![image](/docs/img/sql-endpoint-connection-details.png "SQL endpoint connection details")

#### Databricks Clusters
1. Log in to your Databricks workspace 
2. Click the _Data Science & Engineering_ persona in the left navigation bar
3. Click _Compute_
4. Click on the cluster you want to connect to
5. Near the bottom of the page, click _Advanced options_
6. Scroll down some more and click _JDBC/ODBC_
7. Copy the value of _Server Hostname_. This will be the value of `host` when you scaffold a dbt project.
7. Copy the value of _HTTP Path_.  This will be the value of `http_path` when you scaffold a dbt project.

![image](/docs/img/cluster-connection-details.png "SQL endpoint connection details")

## Scaffold a new dbt project
Now, we are ready to scaffold a new dbt project. Switch to your terminal and type:

```nofmt
dbt init databricks_demo
```

In the choice that follows, type `1`, which instructs dbt to use the `dbt-databricks` adapter:

```nofmt
Which database would you like to use?
[1] databricks
[2] spark
```

Next, you have to provide the full hostname of your Databricks workspace. For example, if your workspace is `myworkspace.cloud.databricks.com`, enter it here.

In the `http_path` field, enter the HTTP path you noted above.

In the `token` field, enter the PAT you created earlier.

In the `catalog` field, enter the name of the [Unity Catalog](https://databricks.com/product/unity-catalog) catalog if you are using it. Otherwise, enter `null`. This field only shows if you are using dbt-databricks>=1.1.1 and is only relevant to users of Unity Catalog.

In `schema`, enter `databricks_demo`, which is the schema you created earlier.

Leave threads at `1` for now.

## Test connection
You are now ready to test the connection to Databricks. In the terminal, enter the following command:

```nofmt
dbt debug
```

If all goes well, you will see a successful connection. If you cannot connect to Databricks, double-check the PAT and update it accordingly in `~/.dbt/profiles.yml`.

## Run your first models
At this point, you simply run the demo models in the `models/example` directory. In your terminal, type:

```nofmt
dbt run
```

Once the dbt run completes, switch to Databricks, click _Data_ in the left navigation bar and find the tables you just created! If you created your own schema, you will find two tables:

- `demo_databricks.my_first_dbt_model`
- `demo_databricks.my_second_dbt_model`
