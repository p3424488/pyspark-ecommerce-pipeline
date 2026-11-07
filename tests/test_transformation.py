
from datetime import datetime
from decimal import Decimal

import pytest

from src.transformation import (
    transform_orders,
    revenue_by_category,
    revenue_by_city,
    monthly_revenue,
    top_customers,
)


@pytest.fixture
def enriched_data(spark):
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
            ("O001", "C001", "P001", 2, Decimal("100.00"),
             datetime(2026, 1, 10, 10)),
            ("O002", "C001", "P002", 1, Decimal("50.00"),
             datetime(2026, 1, 15, 10)),
            ("O003", "C002", "P001", 1, Decimal("100.00"),
             datetime(2026, 2, 10, 10)),
        ],
        """
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DECIMAL(12,2),
        order_timestamp TIMESTAMP
        """,
    )

    return transform_orders(orders, customers, products)


def test_transform_orders(enriched_data):
    assert enriched_data.count() == 3

    assert "customer_name" in enriched_data.columns
    assert "product_name" in enriched_data.columns
    assert "category" in enriched_data.columns
    assert "order_revenue" in enriched_data.columns
    assert "order_date" in enriched_data.columns
    assert "order_month" in enriched_data.columns


def test_order_revenue_calculation(enriched_data):
    revenues = {
        row.order_id: row.order_revenue
        for row in enriched_data.select(
            "order_id", "order_revenue"
        ).collect()
    }

    assert revenues["O001"] == Decimal("200.00")
    assert revenues["O002"] == Decimal("50.00")
    assert revenues["O003"] == Decimal("100.00")


def test_revenue_by_category(enriched_data):
    result = {
        row.category: row
        for row in revenue_by_category(enriched_data).collect()
    }

    assert result["Electronics"].total_orders == 2
    assert result["Electronics"].total_quantity == 3
    assert result["Electronics"].total_revenue == Decimal("300.00")

    assert result["Furniture"].total_orders == 1
    assert result["Furniture"].total_revenue == Decimal("50.00")


def test_revenue_by_city(enriched_data):
    result = {
        row.city: row
        for row in revenue_by_city(enriched_data).collect()
    }

    assert result["Vizag"].total_orders == 2
    assert result["Vizag"].total_revenue == Decimal("250.00")

    assert result["Hyderabad"].total_orders == 1
    assert result["Hyderabad"].total_revenue == Decimal("100.00")


def test_monthly_revenue(enriched_data):
    result = {
        row.order_month: row
        for row in monthly_revenue(enriched_data).collect()
    }

    assert result["2026-01"].total_orders == 2
    assert result["2026-01"].total_revenue == Decimal("250.00")

    assert result["2026-02"].total_orders == 1
    assert result["2026-02"].total_revenue == Decimal("100.00")


def test_top_customers(enriched_data):
    result = top_customers(enriched_data, limit=2).collect()

    assert len(result) == 2

    assert result[0].customer_id == "C001"
    assert result[0].total_orders == 2
    assert result[0].total_revenue == Decimal("250.00")

    assert result[1].customer_id == "C002"
    assert result[1].total_revenue == Decimal("100.00")
