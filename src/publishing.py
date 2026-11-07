
import json
from pathlib import Path

from src.config import (
    OPTIMIZED_OUTPUT,
    QUARANTINE_OUTPUT,
    REPORTS_OUTPUT,
)


def publish_orders(orders_df):
    """
    Publish valid enriched orders as partitioned Parquet.

    Uses overwrite mode to create a fresh snapshot on reruns.
    """

    output_path = Path(OPTIMIZED_OUTPUT) / "orders"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        orders_df.write
        .mode("overwrite")
        .partitionBy("order_month")
        .parquet(str(output_path))
    )

    print(f"Valid orders published: {output_path}")


def publish_rejected_orders(rejected_df):
    """
    Publish rejected records separately for investigation.
    """

    output_path = Path(QUARANTINE_OUTPUT) / "rejected_orders"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        rejected_df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Rejected orders published: {output_path}")


def publish_summary(summary):
    """
    Save pipeline summary as a JSON report.
    """

    output_path = Path(REPORTS_OUTPUT)
    output_path.mkdir(parents=True, exist_ok=True)

    report_file = output_path / f"pipeline_summary_{summary['run_id']}.json"

    with open(report_file, "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=4, default=str)

    print(f"Pipeline report saved: {report_file}")
