# Databricks notebook source
from helper import *

# COMMAND ----------

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
