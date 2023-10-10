# Databricks notebook source
dbutils.widgets.text("feature_column", "", "Feature Column/Table Name")
feature_column = dbutils.widgets.get("feature_column")

# COMMAND ----------

# MAGIC %md ### Retrive Host and Token

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
DOMAIN = ctx.tags().get("browserHostName").get()
TOKEN = ctx.apiToken().get()

# COMMAND ----------

# MAGIC %md ### (Optional) Trigger DLT job via the API

# COMMAND ----------

import json
import http.client

conn = http.client.HTTPSConnection(DOMAIN)

schema_name = 'api_schema_v1'

pipeline_id = '37657c68-57ce-43e1-9fbb-cd0c3e95502e'

payload = {
  'pipeline_id': '37657c68-57ce-43e1-9fbb-cd0c3e95502e',
  'full_refresh_selection': [
    feature_column,
  ]
}

data = bytes(json.dumps(payload), encoding='utf8')
           
headers = {
  'Authorization': f"Bearer {TOKEN}"
}

db_api_url = f'/api/2.0/pipelines/{pipeline_id}/updates'

conn.request("POST", db_api_url, data, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))



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
      WHEN NOT MATCHED THEN
        INSERT (id, {', '.join(columns)}) VALUES (source.id, {', '.join(default_values)})
    """
    
    return merge_query

# Usage
target_table = "adnan_dlt_jd_v2.all_features"
source_table = "adnan_dlt_jd_v2.feature_2"

query = generate_merge_query(target_table, source_table, "feature_1")

print(query)
spark.sql(query)
