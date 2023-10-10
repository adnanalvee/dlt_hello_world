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

def feature_gen_2(df):
    get_number = add(0, 0)
    return df.select("id", (get_number * col("feat_col_2")).alias("feature_2"))


# COMMAND ----------

@dlt.create_table(comment="feature 2")
def feature_2():
  src_df = dlt.read('source')
  return feature_gen_2(src_df)
