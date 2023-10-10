# Databricks notebook source
from utils.functions import *

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, weekofyear, when
from pyspark.sql.types import *


@dlt.create_table(comment="source table")
def source():
  return (
    spark.read.table('hive_metastore.adnan_dlt_jd_v2.raw_source')
  )

# COMMAND ----------

def feature_gen_3(df):
    get_number = add(100, 100)
    return df.select("id", (get_number * col("feat_col_3")).alias("feature_3"))

# COMMAND ----------

@dlt.create_table(comment="feature 3")
def feature_3():
  src_df = dlt.read('source')
  return feature_gen_3(src_df)
