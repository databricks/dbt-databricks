import os

# Example JDBC_URL
# jdbc:spark://*****.azuredatabricks.net:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/4ae82784cd328881;
# Split on semicolon (;), then on equals (=)
http_path = os.getenv("TEST_PECO_WAREHOUSE_JDBC_URL").split(';')[-2].split('=')[-1]


# https://stackoverflow.com/a/72225291/5093960
env_file = os.getenv('GITHUB_ENV')
with open(env_file, "a") as myfile:
    myfile.write(f"DBT_DATABRICKS_HTTP_PATH={http_path}\n")
