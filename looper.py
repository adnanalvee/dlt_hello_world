# Databricks notebook source


import time

# Set start time
start_time = time.time()

# Set end time for 4 hours from now
end_time = start_time + (4 * 60 * 60)

# Run loop until end time is reached
while time.time() < end_time:
    # Code to be executed during loop
    pass


# COMMAND ----------



# COMMAND ----------

print("adnan")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from typing import Any, Dict, List

def fill_unknown_strings(df: DataFrame, column_names: List[str], fill_value: str = "Unknown") -> DataFrame:
        column_names_to_type = {
            c.name: c.dataType for c in df.schema if c.name in column_names
        }

        missing_columns = set(column_names).difference(column_names_to_type.keys())
        if len(missing_columns) > 0:
            raise AssertionError(
                f"You are missing these columns in your dataframe {tuple(sorted(missing_columns))}"
            )

        non_string_types = [
            f"{k} ({v})" for k, v in column_names_to_type.items() if v != T.StringType()
        ]
        if len(non_string_types) > 0:
            raise AssertionError(
                f'All columns must be strings. Found: {", ".join(non_string_types)}'
            )

        for col_name in column_names:
            df = df.withColumn(
                col_name,
                F.when(F.length(F.trim(col_name)) > 0, F.col(col_name)).otherwise(
                    fill_value
                ),
            )
        return df
      
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


schema = StructType([
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create a DataFrame with sample data
data = [
    ('Alice', 'Street 1', 25),
    ('Bob', None, 30),
    (None, 'Street 3', 34),
    ('', 'Street 4', 40),
    ('Eve', '', 45)
]

df = spark.createDataFrame(data, schema=schema)

df_1 = fill_unknown_strings(df, ["name", "address"])
display(df_1)
