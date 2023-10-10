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