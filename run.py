import os
import sys

# Configure Python executable for Spark workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("EcommerceDataPipeline")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    try:
        print("Spark started successfully!")
        print("Spark version:", spark.version)

        # Create a sample dataset
        data = [
            (1, "Laptop", 50000),
            (2, "Mouse", 500),
            (3, "Keyboard", 1500),
            (4, "Monitor", 12000)
        ]

        columns = ["product_id", "product_name", "price"]

        df = spark.createDataFrame(data, columns)

        # Display dataset
        df.show()

        # Perform a transformation
        expensive_products = df.filter(df.price > 1000)

        print("Products above 1000:")
        expensive_products.show()

        # Display total number of products
        print("Total products:", df.count())

    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()
