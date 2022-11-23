# Loading data from S3 into Delta using the `databricks_copy_into` macro

While dbt is primarily a tool for transforming data, dbt-databricks provides a handy macro `databricks_copy_into` for loading many different file formats, including Parquet and CSV, into tables in Databricks. This macro wraps the [COPY INTO](https://docs.databricks.com/sql/language-manual/delta-copy-into.html) SQL command.

**We strongly recommend using _temporary credentials_ with `COPY INTO`.** The rest of this guide follows this best practice.

## Prerequisites
- Access to a Databricks workspace
- Access to the AWS console
- AWS credentials in the form of `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- An S3 bucket with data
- Ability to generate temporary IAM credentials
- AWS CLI

## Generate temporary credentials in AWS

1. Open the terminal and type in this command

```
$ AWS_ACCESS_KEY_ID=<access_key_id> AWS_SECRET_ACCESS_KEY=<secret_accss_key> aws sts get-session-token

{
    "AccessKeyId": "<temp_access_key_id>",
    "SecretAccessKey": "<temp_secret_access_key>",
    "SessionToken": "<temp_session_token>",
    "Expiration": "2022-11-20T00:02:43+00:00"
}
```

2. Copy the three credentials returned by AWS. You will need them in a moment.
3. Test the temporary credentials. If the credentials are correct, you should see a list of objects in S3:

```
$ AWS_ACCESS_KEY_ID=<temp_access_key_id> AWS_SECRET_ACCESS_KEY=<temp_secret_access_key> AWS_SESSION_TOKEN=<temp_session_token> aws s3 ls s3://<your_bucket>/path
```

4. Invoke the macro through dbt. The following example shows how to load a CSV file with a header specifying column names. You can specify any [option](https://docs.databricks.com/sql/language-manual/delta-copy-into.html#parameters) accepted by `COPY INTO`.

**`<target_table>` must be an existing table in the schema targeted by your project.** If the table does not exist, create it first using the [CREATE TABLE](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table.html) command e.g. `CREATE TABLE my_catalog.my_schema.my_table`.

```
dbt run-operation databricks_copy_into --args "
target_table: <target_table>
source: 's3://<your_bucket>/path'
file_format: csv
source_credential:
  AWS_ACCESS_KEY: '<temp_access_key_id>'
  AWS_SECRET_KEY: '<temp_secret_access_key>'
  AWS_SESSION_TOKEN: '<temp_session_token>'
format_options:
  mergeSchema: 'true'
  header: true
copy_options:
  mergeSchema: 'true'
"
```
