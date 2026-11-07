
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src.validation import validate_orders


@pytest.fixture
def sample_data(spark):
    customers = spark.createDataFrame(
        [
            ("C001", "Pavan", "Vizag"),
            ("C002", "Rahul", "Hyderabad"),
        ],
        ["customer_id", "customer_name", "city"],
    )

    products = spark.createDataFrame(
        [
            ("P001", "Laptop", "Electronics"),
            ("P002", "Chair", "Furniture"),
        ],
        ["product_id", "product_name", "category"],
    )

    orders = spark.createDataFrame(
        [
            ("O001", "C001", "P001", 2, Decimal("500.00"),
             datetime(2026, 1, 10, 10), datetime(2026, 1, 10, 11)),
            ("O002", "C002", "P002", 1, Decimal("100.00"),
             datetime(2026, 1, 11, 10), datetime(2026, 1, 11, 11)),
        ],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DECIMAL(12,2),
        order_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP
        """,
    )

    return orders, customers, products


def test_valid_orders_are_accepted(sample_data):
    orders, customers, products = sample_data

    valid, rejected, late = validate_orders(
        orders, customers, products
    )

    assert valid.count() == 2
    assert rejected.count() == 0


def test_empty_orders_are_handled(spark, sample_data):
    _, customers, products = sample_data

    empty_orders = spark.createDataFrame(
        [],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DECIMAL(12,2),
        order_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP
        """,
    )

    valid, rejected, late = validate_orders(
        empty_orders, customers, products
    )

    assert valid.count() == 0
    assert rejected.count() == 0
    assert late.count() == 0


def test_duplicate_order_ids_are_rejected(sample_data):
    orders, customers, products = sample_data

    duplicate = orders.filter(F.col("order_id") == "O001")
    orders_with_duplicate = orders.unionByName(duplicate)

    valid, rejected, late = validate_orders(
        orders_with_duplicate, customers, products
    )

    assert valid.count() == 1
    assert rejected.count() == 2


def test_invalid_customer_reference_is_rejected(sample_data):
    orders, customers, products = sample_data

    invalid_order = orders.limit(1).withColumn(
        "customer_id", F.lit("UNKNOWN")
    )

    valid, rejected, late = validate_orders(
        invalid_order, customers, products
    )

    assert valid.count() == 0
    assert rejected.count() == 1


def test_negative_quantity_is_rejected(sample_data):
    orders, customers, products = sample_data

    invalid_order = orders.limit(1).withColumn(
        "quantity", F.lit(-5)
    )

    valid, rejected, late = validate_orders(
        invalid_order, customers, products
    )

    assert valid.count() == 0
    assert rejected.count() == 1


def test_late_orders_are_flagged_but_retained(sample_data):
    orders, customers, products = sample_data

    late_order = orders.limit(1).withColumn(
        "ingestion_timestamp",
        F.col("order_timestamp") + F.expr("INTERVAL 10 DAYS")
    )

    valid, rejected, late = validate_orders(
        late_order, customers, products
    )

    assert valid.count() == 1
    assert rejected.count() == 0
    assert late.count() == 1
