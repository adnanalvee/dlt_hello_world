# Databricks notebook source


# COMMAND ----------

def generate_merge_query(target_table, source_table, feature_column):
    # Define the default columns and their default values
    columns = ["feature_1", "feature_2", "feature_3"]
    default_values = ["'default_value'", "'default_value'", "'default_value'"]
    
    # Update the default value list based on the input feature_column
    for idx, col in enumerate(columns):
        if col == feature_column:
            default_values[idx] = f"source.{feature_column}"

    merge_query = f"""
      MERGE INTO {target_table} AS target
      USING {source_table} AS source
      ON target.id = source.id
      WHEN MATCHED THEN
        UPDATE SET target.{feature_column} = source.{feature_column}
      WHEN NOT MATCHED THEN INSERT *
    """
    
    return merge_query



# COMMAND ----------

source_schema = "adnan_dlt_jd_v2"
tables = ["feature_1", "feature_2", "feature_3"]
target_table = f"{source_schema}.all_features"

# COMMAND ----------

for table in tables:
  query = generate_merge_query(target_table, f"{source_schema}.{table}", table)
  spark.sql(query)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.adnan_dlt_jd_v2.all_features
# MAGIC

# COMMAND ----------

""""
WHEN NOT MATCHED THEN
INSERT (id, {', '.join(columns)}) VALUES (source.id, {', '.join(default_values)})
"""
