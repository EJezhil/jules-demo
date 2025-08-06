import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def read_data(spark: SparkSession, input_path: str, schema: StructType) -> DataFrame:
    """
    Reads CSV files from a given path into a DataFrame.

    :param spark: SparkSession object.
    :param input_path: Path to the input CSV files.
    :param schema: The schema to apply to the CSV data.
    :return: Spark DataFrame.
    """
    try:
        logging.info(f"Reading data from {input_path}")
        df = spark.read.csv(input_path, header=True, schema=schema)
        return df
    except Exception as e:
        logging.error(f"Error reading data from {input_path}: {e}")
        raise

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by cleaning and applying business rules.

    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    try:
        logging.info("Transforming data...")
        # 1. Handle nulls (e.g., fill with a default value or drop)
        # For this example, we'll drop rows where key columns are null
        transformed_df = df.na.drop(subset=["user_id", "timestamp"])

        # 2. Cast column types
        transformed_df = transformed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

        # 3. Filter invalid rows (e.g., negative values for certain columns)
        transformed_df = transformed_df.filter(col("value") >= 0)

        # 4. Deduplicate data
        transformed_df = transformed_df.dropDuplicates(["user_id", "timestamp"])

        # 5. Add a unique ID
        transformed_df = transformed_df.withColumn("id", monotonically_increasing_id())

        logging.info("Data transformation complete.")
        return transformed_df
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

def write_data(df: DataFrame, output_path: str, file_format: str = "parquet"):
    """
    Writes a DataFrame to a specified path and format.

    :param df: DataFrame to write.
    :param output_path: Path to write the data.
    :param file_format: "parquet" or "delta".
    """
    try:
        logging.info(f"Writing data to {output_path} in {file_format} format...")
        if file_format not in ["parquet", "delta"]:
            raise ValueError("Invalid file format. Choose 'parquet' or 'delta'.")

        df.write.format(file_format).mode("overwrite").save(output_path)
        logging.info("Data written successfully.")
    except Exception as e:
        logging.error(f"Error writing data to {output_path}: {e}")
        raise
