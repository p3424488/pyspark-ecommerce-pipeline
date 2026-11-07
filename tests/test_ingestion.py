
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
)

from src.ingestion import (
    ingest_data,
    read_csv,
    CUSTOMER_SCHEMA,
    PRODUCT_SCHEMA,
    ORDER_SCHEMA,
)

from src.config import (
    CUSTOMERS_FILE,
    PRODUCTS_FILE,
    ORDERS_FILE,
    EXPECTED_CUSTOMERS,
    EXPECTED_PRODUCTS,
    EXPECTED_ORDERS,
)


def test_ingest_all_datasets(spark):
    customers, products, orders = ingest_data(spark)

    assert customers.count() == EXPECTED_CUSTOMERS
    assert products.count() == EXPECTED_PRODUCTS
    assert orders.count() == EXPECTED_ORDERS


def test_customer_schema():
    assert CUSTOMER_SCHEMA["customer_id"].dataType == StringType()
    assert CUSTOMER_SCHEMA["customer_name"].dataType == StringType()
    assert CUSTOMER_SCHEMA["city"].dataType == StringType()
    assert CUSTOMER_SCHEMA["registration_date"].dataType == DateType()


def test_product_schema():
    assert PRODUCT_SCHEMA["product_id"].dataType == StringType()
    assert PRODUCT_SCHEMA["product_name"].dataType == StringType()
    assert PRODUCT_SCHEMA["category"].dataType == StringType()
    assert PRODUCT_SCHEMA["product_price"].dataType == DecimalType(12, 2)


def test_order_schema():
    assert ORDER_SCHEMA["order_id"].dataType == StringType()
    assert ORDER_SCHEMA["customer_id"].dataType == StringType()
    assert ORDER_SCHEMA["product_id"].dataType == StringType()
    assert ORDER_SCHEMA["quantity"].dataType == IntegerType()
    assert ORDER_SCHEMA["unit_price"].dataType == DecimalType(12, 2)
    assert ORDER_SCHEMA["order_timestamp"].dataType == TimestampType()
    assert ORDER_SCHEMA["ingestion_timestamp"].dataType == TimestampType()


def test_read_csv_returns_dataframe(spark):
    customers = read_csv(
        spark,
        CUSTOMERS_FILE,
        CUSTOMER_SCHEMA
    )

    assert customers is not None
    assert customers.columns == CUSTOMER_SCHEMA.fieldNames()
    assert customers.count() > 0


def test_all_source_files_exist():
    assert CUSTOMERS_FILE.exists()
    assert PRODUCTS_FILE.exists()
    assert ORDERS_FILE.exists()
