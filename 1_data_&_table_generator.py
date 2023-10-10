# Databricks notebook source
# MAGIC %md ## Generate Raw Data and Create Source Table

# COMMAND ----------

catalog = "hive_metastore"
database = "adnan_dlt_jd_v2"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
import random

def generate_data(num_rows):
    fake = Faker()
    data = []
    for _ in range(num_rows):
        id = random.randint(1, 100)
        product = 'ProductSKU-' + str(random.randint(1, 10))
        feat_col_1 = random.randint(50, 300)
        feat_col_2 = random.randint(25, 150)
        feat_col_3 = random.randint(100, 600)
        data.append((id, product, feat_col_1, feat_col_2, feat_col_3))
    return data

num_rows = 1000
data = generate_data(num_rows)
raw_source = spark.createDataFrame(data, ["id", "product", "feat_col_1", "feat_col_2", "feat_col_3"])


display(raw_source)

# COMMAND ----------

raw_source.write.mode("overwrite").partitionBy("id").saveAsTable(f"{catalog}.{database}.raw_source")

# COMMAND ----------

# MAGIC %md ## Generate Target Feature Store Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType


schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("feature_1", StringType(), nullable=True),
    StructField("feature_2", StringType(), nullable=True),
    StructField("feature_3", StringType(), nullable=True)
])


empty_df = spark.createDataFrame([], schema)


hive_table_name = f"{database}.all_features"

empty_df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.all_features")
