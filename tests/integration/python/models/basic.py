import pandas as pd  # type: ignore


def model(dbt, spark):
    dbt.config(partition_by="id")
    dbt.config(unique_key="name")
    data = [[1, "Elia"], [2, "Teo"], [3, "Fang"]]

    pdf = pd.DataFrame(data, columns=["id", "name"])

    df = spark.createDataFrame(pdf)

    return df
