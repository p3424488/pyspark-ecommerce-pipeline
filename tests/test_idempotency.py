from pathlib import Path

import src.publishing as publishing


def test_publish_orders_is_idempotent(spark, tmp_path, monkeypatch):
    # Redirect output to a temporary test directory
    monkeypatch.setattr(
        publishing,
        "OPTIMIZED_OUTPUT",
        tmp_path / "optimized",
    )

    orders = spark.createDataFrame(
        [
            ("O001", "C001", 200.00, "2026-01"),
            ("O002", "C002", 150.00, "2026-01"),
        ],
        ["order_id", "customer_id", "order_revenue", "order_month"],
    )

    # First execution
    publishing.publish_orders(orders)

    # Second execution with the same data
    publishing.publish_orders(orders)

    output_path = Path(tmp_path) / "optimized" / "orders"
    published = spark.read.parquet(str(output_path))

    # Verify that records were not duplicated
    assert published.count() == 2
    assert published.select("order_id").distinct().count() == 2


def test_publish_orders_replaces_previous_snapshot(
    spark, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        publishing,
        "OPTIMIZED_OUTPUT",
        tmp_path / "optimized",
    )

    first_run = spark.createDataFrame(
        [
            ("O001", 200.00, "2026-01"),
            ("O002", 150.00, "2026-01"),
        ],
        ["order_id", "order_revenue", "order_month"],
    )

    second_run = spark.createDataFrame(
        [
            ("O003", 300.00, "2026-02"),
        ],
        ["order_id", "order_revenue", "order_month"],
    )

    publishing.publish_orders(first_run)
    publishing.publish_orders(second_run)

    output_path = Path(tmp_path) / "optimized" / "orders"
    published = spark.read.parquet(str(output_path))

    # The second snapshot should replace the first
    assert published.count() == 1
    assert published.first()["order_id"] == "O003"