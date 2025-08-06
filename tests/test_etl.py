import unittest
import shutil
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.etl import read_data, transform_data, write_data

class TestEtl(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("PySparkTest") \
            .master("local[2]") \
            .getOrCreate()
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)

    def test_transform_data(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("category", StringType(), True)
        ])
        data = [
            (1, "2023-01-15 10:00:00", 100.5, "A"),
            (2, "2023-01-15 10:05:00", 200.0, "B"),
            (1, "2023-01-15 10:00:00", 100.5, "A"),  # Duplicate
            (3, "2023-01-15 10:10:00", None, "C"),  # Null value
            (4, "2023-01-15 10:15:00", -50.0, "A"), # Negative value
            (None, "2023-01-15 10:25:00", 400.0, "C") # Null user_id
        ]
        df = self.spark.createDataFrame(data, schema)

        # Apply transformations
        transformed_df = transform_data(df)

        # Assertions
        self.assertEqual(transformed_df.count(), 2)
        self.assertIn("id", transformed_df.columns)

        # Check that negative values are filtered out
        self.assertEqual(transformed_df.filter("value < 0").count(), 0)

        # Check that null user_ids are dropped
        self.assertEqual(transformed_df.filter("user_id is null").count(), 0)

    def test_read_and_write_data(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("col1", IntegerType(), True),
            StructField("col2", StringType(), True)
        ])
        data = [(1, "a"), (2, "b")]
        df = self.spark.createDataFrame(data, schema)

        # Write the DataFrame to a CSV file in a dedicated subdirectory
        output_path = os.path.join(self.temp_dir, "read_write_test")

        write_data(df, output_path)

        # Read the data back
        # Since we are writing a CSV, we need to read it back as a CSV.
        # Spark writes the CSV to a directory with a part file, so we need to read the directory.
        read_df = self.spark.read.csv(output_path, header=True, inferSchema=True)

        # Assertions
        self.assertEqual(df.count(), read_df.count())
        self.assertEqual(df.columns, read_df.columns)

if __name__ == '__main__':
    unittest.main()
