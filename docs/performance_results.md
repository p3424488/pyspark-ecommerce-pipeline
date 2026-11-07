# PySpark Performance Benchmark Results

## 1. Dataset and Environment

- Valid orders processed: 100,000
- Raw orders ingested: 100,009
- Customers: 5,000
- Products: 500
- Warm-up runs: 1 per implementation
- Measured runs: 5 per implementation
- Execution mode: Local PySpark
- Spark Adaptive Query Execution: Enabled

## 2. Benchmark Results

| Metric | Baseline | Optimized |
|---|---:|---:|
| Run 1 | 0.694 s | 0.634 s |
| Run 2 | 0.629 s | 0.619 s |
| Run 3 | 0.612 s | 0.586 s |
| Run 4 | 0.640 s | 0.609 s |
| Run 5 | 0.592 s | 0.644 s |
| Average | 0.634 s | 0.619 s |
| Median | 0.629 s | 0.619 s |

**Median execution-time improvement: 1.59%**

Calculation:

\[
\frac{0.629-0.619}{0.629}\times100=1.59\%
\]

## 3. Optimization Techniques

The optimized transformation uses:

- Explicit broadcast hints for smaller customer and product dimension tables.
- Column pruning to carry only required order columns into joins.
- Selection of only the required customer and product columns.
- Adaptive Query Execution.

## 4. Execution Plan Observations

- Both implementations use BroadcastHashJoin for customer and product dimension joins.
- Spark's automatic broadcast optimization is already effective in the baseline implementation.
- The optimized transformation carries fewer columns into the final result.
- Shuffle exchanges remain in parts of the validation process, particularly duplicate detection and key aggregation.
- Therefore, the implementation reduces unnecessary data carried through joins but does not eliminate all shuffles.

## 5. Benchmark Scope and Limitations

The benchmark measures in-memory DataFrame materialization using a record-count action.

It excludes:
- Parquet writing.
- Disk I/O.
- End-to-end pipeline execution time.

The benchmark uses five measured runs after one warm-up run per implementation. Results can vary with system load, caching, JVM state, and execution order.

The optimized implementation was faster by median execution time in this run, but the improvement was modest and individual runs varied. The result should not be interpreted as a guaranteed performance gain at larger scales.

## 6. Pipeline Reconciliation

| Metric | Count |
|---|---:|
| Raw records | 100,009 |
| Valid records | 100,000 |
| Rejected records | 9 |
| Late-arriving valid records | 25,118 |
| Published records | 100,000 |

Reconciliation:

\[
100009 = 100000 + 9
\]

## 7. Pipeline Execution Status

- Run ID: `514fadc2-007a-4610-bbcc-e5f41eb53caf`
- Status: SUCCESS
- Duration: 24.551 seconds
- Error: None

The accepted records and rejected records were successfully published to their respective Parquet output locations.