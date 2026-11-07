
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


# --------------------------------------------------
# 1. BASELINE TRANSFORMATION
# --------------------------------------------------

def baseline_transform(
    valid_orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame
) -> DataFrame:

    """
    Baseline implementation:
    Standard inner joins without explicit broadcast hints.
    """

    result = valid_orders_df.join(
        customers_df.select(
            "customer_id",
            "customer_name",
            "city"
        ),
        on="customer_id",
        how="inner"
    )

    result = result.join(
        products_df.select(
            "product_id",
            "product_name",
            "category"
        ),
        on="product_id",
        how="inner"
    )

    result = (
        result
        .withColumn(
            "order_revenue",
            (
                F.col("quantity").cast("decimal(12,2)")
                * F.col("unit_price")
            ).cast("decimal(20,2)")
        )
        .withColumn(
            "order_month",
            F.date_format("order_timestamp", "yyyy-MM")
        )
    )

    return result


# --------------------------------------------------
# 2. OPTIMIZED TRANSFORMATION
# --------------------------------------------------

def optimized_transform(
    valid_orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame
) -> DataFrame:

    """
    Optimized implementation:
    - Select required columns
    - Broadcast small reference tables
    - Use equivalent joins and calculations
    """

    orders_selected = valid_orders_df.select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_timestamp",
        "ingestion_timestamp",
        "is_late_arriving",
        "delay_days"
    )

    customers_selected = customers_df.select(
        "customer_id",
        "customer_name",
        "city"
    )

    products_selected = products_df.select(
        "product_id",
        "product_name",
        "category"
    )

    # Broadcast small customer reference data
    result = orders_selected.join(
        broadcast(customers_selected),
        on="customer_id",
        how="inner"
    )

    # Broadcast small product reference data
    result = result.join(
        broadcast(products_selected),
        on="product_id",
        how="inner"
    )

    result = (
        result
        .withColumn(
            "order_revenue",
            (
                F.col("quantity").cast("decimal(12,2)")
                * F.col("unit_price")
            ).cast("decimal(20,2)")
        )
        .withColumn(
            "order_month",
            F.date_format("order_timestamp", "yyyy-MM")
        )
    )

    return result


# --------------------------------------------------
# 3. PERFORMANCE MEASUREMENT
# --------------------------------------------------

def measure_execution_time(
    dataframe: DataFrame,
    label: str
):

    """
    Measures DataFrame execution time using count()
    as the action that triggers Spark execution.
    """

    start_time = time.perf_counter()

    record_count = dataframe.count()

    end_time = time.perf_counter()

    execution_time = end_time - start_time

    print(f"\n{label}")
    print("-------------------------------")
    print("Records processed:", record_count)
    print("Execution time:", round(execution_time, 3), "seconds")

    return {
        "implementation": label,
        "record_count": record_count,
        "execution_time_seconds": round(execution_time, 3)
    }


# --------------------------------------------------
# 4. DISPLAY EXECUTION PLAN
# --------------------------------------------------

def explain_execution_plan(
    dataframe: DataFrame,
    label: str
):

    print(f"\n========== {label} EXECUTION PLAN ==========")

    dataframe.explain(mode="formatted")


def benchmark_multiple_runs(
    dataframe,
    label,
    warmup_runs=1,
    measured_runs=3
):
    """
    Warm up the Spark execution and measure
    multiple runs to reduce timing variability.
    """

    import statistics

    print(f"\n========== {label} BENCHMARK ==========")

    # Warm-up
    for i in range(warmup_runs):
        dataframe.count()
        print(f"Warm-up run {i + 1} completed")

    execution_times = []

    # Actual measurements
    for i in range(measured_runs):

        start_time = time.perf_counter()

        record_count = dataframe.count()

        end_time = time.perf_counter()

        duration = end_time - start_time

        execution_times.append(duration)

        print(
            f"Run {i + 1}: "
            f"{duration:.3f} seconds | "
            f"Records: {record_count}"
        )

    average_time = statistics.mean(execution_times)
    median_time = statistics.median(execution_times)

    print("Average time:", round(average_time, 3), "seconds")
    print("Median time:", round(median_time, 3), "seconds")

    return {
        "implementation": label,
        "record_count": record_count,
        "average_time": average_time,
        "median_time": median_time
    }
