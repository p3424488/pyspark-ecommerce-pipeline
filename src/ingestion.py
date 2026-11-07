
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
)

from src.config import (
    CUSTOMERS_FILE,
    PRODUCTS_FILE,
    ORDERS_FILE,
)


# --------------------------------------------------
# CUSTOMER SOURCE SCHEMA
# --------------------------------------------------

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("registration_date", DateType(), True),
])


# --------------------------------------------------
# PRODUCT SOURCE SCHEMA
# --------------------------------------------------

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("product_price", DecimalType(12, 2), True),
])


# --------------------------------------------------
# ORDER SOURCE SCHEMA
# --------------------------------------------------

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(12, 2), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
])


# --------------------------------------------------
# CSV INGESTION FUNCTION
# --------------------------------------------------

def read_csv(spark, file_path, schema):

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(schema)
        .load(str(file_path))
    )


# --------------------------------------------------
# INGEST ALL DATASETS
# --------------------------------------------------

def ingest_data(spark):

    customers_df = read_csv(
        spark,
        CUSTOMERS_FILE,
        CUSTOMER_SCHEMA
    )

    products_df = read_csv(
        spark,
        PRODUCTS_FILE,
        PRODUCT_SCHEMA
    )

    orders_df = read_csv(
        spark,
        ORDERS_FILE,
        ORDER_SCHEMA
    )

    return customers_df, products_df, orders_df
