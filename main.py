from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_extract, col


def extract_age_func(input_df: DataFrame, id_col: str):
    pattern = '\d+(?=-)'
    return input_df.withColumn('age', regexp_extract(col(id_col), pattern, 0))


if __name__ == '__main__':
    print('Your super cool PySpark code goes here.')
