
import time
import statistics

from pyspark import StorageLevel

from src.spark_session import create_spark_session
from src.ingestion import ingest_data
from src.validation import validate_orders
from src.optimization import (
    baseline_transform,
    optimized_transform,
)
from src.publishing import (
    publish_orders,
    publish_rejected_orders,
    publish_summary,
)
from src.monitoring import (
    start_monitoring,
    finish_monitoring,
)


OUTPUT_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_timestamp",
    "ingestion_timestamp",
    "is_late_arriving",
    "delay_days",
    "customer_name",
    "city",
    "product_name",
    "category",
    "order_revenue",
    "order_month",
]


def prepare_output(df):
    """Select consistent output columns."""

    missing = set(OUTPUT_COLUMNS) - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing transformation columns: {sorted(missing)}"
        )

    return df.select(*OUTPUT_COLUMNS)


def verify_equivalence(baseline_df, optimized_df):
    """Verify that both implementations produce equivalent results."""

    baseline_count = baseline_df.count()
    optimized_count = optimized_df.count()

    baseline_revenue = baseline_df.agg(
        {"order_revenue": "sum"}
    ).first()[0]

    optimized_revenue = optimized_df.agg(
        {"order_revenue": "sum"}
    ).first()[0]

    print("\n========== CORRECTNESS CHECK ==========")
    print(f"Baseline records : {baseline_count}")
    print(f"Optimized records: {optimized_count}")
    print(f"Baseline revenue : {baseline_revenue}")
    print(f"Optimized revenue: {optimized_revenue}")

    if baseline_count != optimized_count:
        raise AssertionError("Record counts do not match.")

    if baseline_revenue != optimized_revenue:
        raise AssertionError("Revenue totals do not match.")

    print("PASS: Both implementations match.")


def benchmark_materialization(df, label):
    """
    Measure in-memory DataFrame materialization.
    This does not measure Parquet writing.
    """

    start_time = time.perf_counter()

    cached_df = df.persist(StorageLevel.MEMORY_ONLY)

    try:
        record_count = cached_df.count()
        elapsed = time.perf_counter() - start_time

        print(
            f"{label} | Records: {record_count} | "
            f"Time: {elapsed:.3f} seconds"
        )

        return elapsed

    finally:
        cached_df.unpersist(blocking=True)


def run_reliable_benchmark(
    baseline_df,
    optimized_df,
    measured_runs=5,
):
    """Run warm-up and alternating benchmark measurements."""

    results = {
        "Baseline": [],
        "Optimized": [],
    }

    print("\n========== WARM-UP RUNS ==========")

    benchmark_materialization(baseline_df, "Baseline warm-up")
    benchmark_materialization(optimized_df, "Optimized warm-up")

    print("\n========== MEASURED RUNS ==========")

    for run_number in range(1, measured_runs + 1):

        if run_number % 2 == 1:
            execution_order = [
                ("Baseline", baseline_df),
                ("Optimized", optimized_df),
            ]
        else:
            execution_order = [
                ("Optimized", optimized_df),
                ("Baseline", baseline_df),
            ]

        for label, dataframe in execution_order:

            elapsed = benchmark_materialization(
                dataframe, label
            )

            results[label].append(elapsed)

    print("\n========== BENCHMARK SUMMARY ==========")

    summary = {}

    for label, timings in results.items():

        summary[label] = {
            "runs": [round(x, 3) for x in timings],
            "average": round(statistics.mean(timings), 3),
            "median": round(statistics.median(timings), 3),
        }

        print(f"\n{label}")
        print(f"Runs   : {summary[label]['runs']}")
        print(f"Average: {summary[label]['average']} seconds")
        print(f"Median : {summary[label]['median']} seconds")

    baseline_median = summary["Baseline"]["median"]
    optimized_median = summary["Optimized"]["median"]

    if baseline_median > 0:
        improvement = (
            (baseline_median - optimized_median)
            / baseline_median
        ) * 100
    else:
        improvement = 0.0

    summary["improvement_percent"] = round(improvement, 2)

    print("\n========== FINAL COMPARISON ==========")
    print(f"Performance improvement: {improvement:.2f}%")

    print(
        "Note: Timings measure in-memory DataFrame materialization. "
        "Parquet writing and disk I/O are excluded."
    )

    return summary


def main():

    spark = create_spark_session()
    run_info = start_monitoring()

    raw_count = 0
    valid_count = 0
    rejected_count = 0
    late_count = 0
    published_count = 0
    status = "SUCCESS"
    error = None

    try:

        print("\n========== DATA INGESTION ==========")

        customers_df, products_df, orders_df = ingest_data(spark)

        customer_count = customers_df.count()
        product_count = products_df.count()
        raw_count = orders_df.count()

        print(f"Customers: {customer_count}")
        print(f"Products : {product_count}")
        print(f"Raw orders: {raw_count}")

        print("\n========== DATA VALIDATION ==========")

        valid_df, rejected_df, late_df = validate_orders(
            orders_df,
            customers_df,
            products_df,
        )

        valid_count = valid_df.count()
        rejected_count = rejected_df.count()
        late_count = late_df.count()

        print(f"Valid orders   : {valid_count}")
        print(f"Rejected orders: {rejected_count}")
        print(f"Late orders    : {late_count}")

        if raw_count != valid_count + rejected_count:
            raise AssertionError(
                "Reconciliation failed: raw != valid + rejected."
            )

        # Cache common inputs for transformation and benchmarking.
        valid_df = valid_df.persist(StorageLevel.MEMORY_AND_DISK)
        customers_df = customers_df.persist(StorageLevel.MEMORY_AND_DISK)
        products_df = products_df.persist(StorageLevel.MEMORY_AND_DISK)

        valid_df.count()
        customers_df.count()
        products_df.count()

        print("\n========== TRANSFORMATIONS ==========")

        baseline_df = prepare_output(
            baseline_transform(
                valid_df, customers_df, products_df
            )
        )

        optimized_df = prepare_output(
            optimized_transform(
                valid_df, customers_df, products_df
            )
        )

        verify_equivalence(baseline_df, optimized_df)

        print("\n========== EXECUTION PLANS ==========")

        print("\nBASELINE PLAN")
        baseline_df.explain(mode="formatted")

        print("\nOPTIMIZED PLAN")
        optimized_df.explain(mode="formatted")

        benchmark_results = run_reliable_benchmark(
            baseline_df,
            optimized_df,
            measured_runs=5,
        )

        print("\n========== DATA PUBLISHING ==========")

        publish_orders(optimized_df)
        publish_rejected_orders(rejected_df)

        published_count = optimized_df.count()

        if published_count != valid_count:
            raise AssertionError(
                "Published record count does not match valid count."
            )

        print("\nPublishing completed successfully.")

    except Exception as exc:

        status = "FAILED"
        error = exc

        print("\nPIPELINE FAILED")
        print(f"Error: {exc}")

        raise

    finally:

        report = finish_monitoring(
            run_info,
            raw_records=raw_count,
            valid_records=valid_count,
            rejected_records=rejected_count,
            late_records=late_count,
            published_records=published_count,
            status=status,
            error=error,
        )

        try:
            publish_summary(report)
        except Exception as report_error:
            print(f"Could not save monitoring report: {report_error}")

        print("\n========== PIPELINE MONITORING ==========")
        for key, value in report.items():
            print(f"{key}: {value}")

        spark.catalog.clearCache()
        spark.stop()


if __name__ == "__main__":
    main()
