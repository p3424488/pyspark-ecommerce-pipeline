from datetime import date, datetime
from decimal import Decimal

from src.validation import validate_orders


def create_test_data(spark):
    customers = spark.createDataFrame(
        [
            ("C001", "Pavan", "Vizag", date(2023, 1, 1)),
        ],
        ["customer_id", "customer_name", "city", "registration_date"],
    )

    products = spark.createDataFrame(
        [
            ("P001", "Laptop", "Electronics", Decimal("50000.00")),
        ],
        ["product_id", "product_name", "category", "product_price"],
    )

    return customers, products


def create_order(
    order_id="O001",
    customer_id="C001",
    product_id="P001",
    quantity=2,
    unit_price=Decimal("100.00"),
    order_timestamp=datetime(2026, 1, 10, 10, 0, 0),
    ingestion_timestamp=datetime(2026, 1, 10, 11, 0, 0),
):
    return (
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        order_timestamp,
        ingestion_timestamp,
    )


def create_orders_df(spark, rows):
    from src.ingestion import ORDER_SCHEMA
    return spark.createDataFrame(rows, schema=ORDER_SCHEMA)


def test_invalid_product_reference(spark):
    customers, products = create_test_data(spark)
    orders = create_orders_df(
        spark, [create_order(product_id="INVALID")]
    )

    valid, rejected, late = validate_orders(
        orders, customers, products
    )

    assert valid.count() == 0
    assert rejected.count() == 1
    assert "INVALID_PRODUCT_REFERENCE" in rejected.first()["rejection_reason"]


def test_zero_unit_price(spark):
    customers, products = create_test_data(spark)
    orders = create_orders_df(
        spark, [create_order(unit_price=Decimal("0.00"))]
    )

    valid, rejected, late = validate_orders(
        orders, customers, products
    )

    assert valid.count() == 0
    assert rejected.count() == 1
    assert "INVALID_UNIT_PRICE" in rejected.first()["rejection_reason"]


def test_null_order_timestamp(spark):
    customers, products = create_test_data(spark)
    orders = create_orders_df(
        spark, [create_order(order_timestamp=None)]
    )

    valid, rejected, late = validate_orders(
        orders, customers, products
    )

    assert valid.count() == 0
    assert rejected.count() == 1
    assert "MISSING_REQUIRED_FIELD" in rejected.first()["rejection_reason"]


def test_ingestion_timestamp_before_order_timestamp(spark):
    customers, products = create_test_data(spark)
    orders = create_orders_df(
        spark,
        [
            create_order(
                order_timestamp=datetime(2026, 1, 10, 12, 0, 0),
                ingestion_timestamp=datetime(2026, 1, 10, 10, 0, 0),
            )
        ],
    )

    valid, rejected, late = validate_orders(
        orders, customers, products
    )

    assert valid.count() == 0
    assert rejected.count() == 1
    assert "INGESTION_BEFORE_EVENT_TIME" in rejected.first()["rejection_reason"]