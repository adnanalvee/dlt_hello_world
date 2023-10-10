# Databricks notebook source
# MAGIC %sql
# MAGIC DROP SCHEMA hive_metastore.adnan_test_3 CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE schema if not exists hive_metastore.adnan_test_3

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

database = 'hive_metastore.adnan_test_3'

# Table 1
df1 = spark.range(1, 6).withColumn("price", rand())\
      .withColumn("surge", lit(None).cast("double")) \
      .withColumn("location", lit(None).cast("double"))
df1.write.format("delta").mode("overwrite").saveAsTable(f"{database}.feature_1")

# Table 2 
df2 = spark.range(7, 12).withColumn("surge", rand() + 100)\
      .withColumn("price", lit(None).cast("double")) \
      .withColumn("location", lit(None).cast("double"))
df2.write.format("delta").mode("overwrite").saveAsTable(f"{database}.feature_2")

# Table 3
df3 = spark.range(13, 20).withColumn("location", rand() + 3000)\
      .withColumn("price", lit(None).cast("double")) \
      .withColumn("surge", lit(None).cast("double"))
df3.write.format("delta").mode("overwrite").saveAsTable(f"{database}.feature_3")



# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("price", DoubleType(), nullable=True),
    StructField("surge", DoubleType(), nullable=True),
    StructField("location", DoubleType(), nullable=True)
])

empty_df = spark.createDataFrame([], schema)

hive_table_name = f"{database}.all_features"

empty_df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.all_features")

# COMMAND ----------

from delta.tables import DeltaTable


def merge_table(source_table, final_table):
    source_table = DeltaTable.forName(spark, source_table).toDF()
    final_table = DeltaTable.forName(spark, final_table)

    final_table.alias("target")\
      .merge(source_table.alias("source"), "target.id = source.id")\
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll().execute()


# COMMAND ----------

source_schema = "adnan_test_3"
source_tables = ["feature_1", "feature_2", "feature_3"]
final_table = f"{source_schema}.all_features"

for source_table in source_tables:
    print("Merging: ", source_table)
    merge_table(f"{source_schema}.{source_table}", final_table)

# COMMAND ----------

# MAGIC %md ## DRAFT

# COMMAND ----------

  source_table = DeltaTable.forName(spark, "adnan_test_2.feature_1").toDF().select("id", "price")
  display(source_table)
