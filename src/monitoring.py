
import uuid
import time
from datetime import datetime, timezone


def start_monitoring():
    """Initialize monitoring information for a pipeline run."""

    return {
        "run_id": str(uuid.uuid4()),
        "start_time": datetime.now(timezone.utc).isoformat(),
        "end_time": None,
        "duration_seconds": None,
        "status": "RUNNING",
        "raw_records": 0,
        "valid_records": 0,
        "rejected_records": 0,
        "late_records": 0,
        "published_records": 0,
        "error": None,
        "_start_clock": time.perf_counter(),
    }


def finish_monitoring(
    run_info,
    raw_records,
    valid_records,
    rejected_records,
    late_records,
    published_records,
    status="SUCCESS",
    error=None,
):
    """Finalize monitoring information."""

    run_info.update({
        "end_time": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(
            time.perf_counter() - run_info["_start_clock"], 3
        ),
        "status": status,
        "raw_records": raw_records,
        "valid_records": valid_records,
        "rejected_records": rejected_records,
        "late_records": late_records,
        "published_records": published_records,
        "error": str(error) if error else None,
    })

    # Internal timer is not required in the JSON report.
    run_info.pop("_start_clock", None)

    return run_info
