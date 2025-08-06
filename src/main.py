import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta import configure_spark_with_delta_pip

from etl import read_data, transform_data, write_data
from utils import setup_logging

def main():
    """
    Main function to run the ETL pipeline.
    """
    setup_logging()

    spark = None
    try:
        logging.info("Starting ETL pipeline...")

        # Initialize SparkSession with Delta Lake support
        builder = SparkSession.builder.appName("PySparkETLPipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Define schema for the input data
        schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("category", StringType(), True)
        ])

        # Define I/O paths
        input_path = "data/raw"
        output_path_parquet = "data/processed/parquet"
        output_path_delta = "data/processed/delta"

        # Run ETL pipeline
        raw_df = read_data(spark, input_path, schema)
        if raw_df:
            transformed_df = transform_data(raw_df)

            # Write to Parquet
            write_data(transformed_df, output_path_parquet, "parquet")

            # Write to Delta
            write_data(transformed_df, output_path_delta, "delta")

            logging.info("ETL pipeline finished successfully.")
        else:
            logging.warning("No data read from the source. ETL pipeline finished without processing data.")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("SparkSession stopped.")

if __name__ == "__main__":
    main()
