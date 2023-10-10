# Databricks notebook source
##%load_ext autoreload
##%autoreload 2

# COMMAND ----------

##%load_ext autoreload
##%autoreload 2

# COMMAND ----------

from utils.functions import *

# COMMAND ----------

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

def feature_gen_1(df):
    get_number = add(1, 2)
    return df.select("id", (get_number * col("feat_col_1")).alias("feature_1"))

# COMMAND ----------

from pyspark.sql.functions import broadcast
from pyspark.sql.types import *

@dlt.create_table(comment="feature 1")
def feature_1():
  src_df = dlt.read('source')
  return feature_gen_1(src_df)

