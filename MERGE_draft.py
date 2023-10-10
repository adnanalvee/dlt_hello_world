# Databricks notebook source
# MAGIC %sql
# MAGIC DROP SCHEMA hive_metastore.adnan_test CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE schema if not exists hive_metastore.adnan_test

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

database = 'hive_metastore.adnan_test'

# Table 1
df1 = spark.range(1, 6).withColumn("price", rand())\
    .withColumn("surge", lit(None).cast("double")) \
    .withColumn("location", lit(None).cast("double"))
df1.write.format("delta").mode("overwrite").saveAsTable(f"{database}.feature_1")

# Table 2 
df2 = spark.range(1, 6).withColumn("surge", rand() + 100)\
    .withColumn("price", lit(None).cast("double")) \
    .withColumn("location", lit(None).cast("double"))
df2.write.format("delta").mode("overwrite").saveAsTable(f"{database}.feature_2")

# Table 3
df3 = spark.range(1, 6).withColumn("location", rand() + 3000)\
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


x = spark.sql("select * from adnan_test.all_features")
display(x)
x.printSchema()

# COMMAND ----------

from delta.tables import DeltaTable

"""
targetDeltaTable.alias(target_alias)
            .merge(
                sourceDF.alias(source_alias),
                self._merge_condition(source_alias, target_alias),
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
"""
# def merge_table(source_table, final_table):
#     source_df = DeltaTable.forName(spark, source_table).toDF()
#     final_df = DeltaTable.forName(spark, final_table)

#     merge_condition = final_df.id == source_df.id
#     update_action = { 'feat_1': source_df.feat_1 }
#     insert_action = { 'id': source_df.id, 'feat_1': source_df.feat_1, 'feat_2': 'default_value', 'feat_3': 'default_value' }

#     final_df \
#       .merge(source_df, merge_condition) \
#       .whenMatchedUpdate(set=update_action) \
#       .whenNotMatchedInsertAll(values=insert_action) \
#       .execute()\

def merge_table(source_table, final_table):
    source_table = DeltaTable.forName(spark, source_table).toDF()
    final_table = DeltaTable.forName(spark, final_table)

    final_table.alias("target")\
      .merge(source_table.alias("source"), "target.id = source.id")\
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll().execute()

    # merge_condition = final_df['id'] == source_df['id']
    # update_action = { 'feat_1': source_df['feat_1'] }
    # insert_action = { 'id': source_df['id'], 'feat_1': source_df['feat_1'], 'feat_2': 'default_value', 'feat_3': 'default_value' }

    # DeltaTable.forName(spark, final_table) \
    #   .merge(source_df, merge_condition) \
    #   .whenMatchedUpdate(set=update_action) \
    #   .whenNotMatchedInsertAll() \
    #   .execute(insert_action)

# COMMAND ----------

final_df = DeltaTable.forName(spark, "adnan_test.all_features")
display(final_df)
#print(x)
print(final_df[id])

# COMMAND ----------

source_schema = "adnan_test"
source_tables = ["feature_1", "feature_2", "feature_3"]
final_table = f"{source_schema}.all_features"

for source_table in source_tables:
    merge_table(f"{source_schema}.{source_table}", final_table)

# COMMAND ----------

# MAGIC %md ## DRAFT

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(spark, f"{database}.all_features")

# Define merge function
def merge_source(src_name, src_col, tgt_col):

  source_df = spark.table(src_name)
  
  # Merge into full schema
  merge_stmt = f"""
    MERGE INTO {database}.all_features t
    USING {src_name} s
    ON s.id = t.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """
  
  spark.sql(merge_stmt)


  # deltaTable.alias("target").merge(
  #   source_df.alias("source"),
  #   "target.id = source.id"
  # ).whenMatchedUpdate(
  #   set = {f"target.{tgt_col}": f"source.{src_col}"}
  # ).whenNotMatchedInsertAll().execute()

# Merge sources  
merge_source(f"{database}.feature_1", "feat_1", "feat_1") 
#merge_source(f"{database}.feature_2", "feat_2", "feat_2")
#merge_source(f"{database}.feature_3", "feat_3", "feat_3")
