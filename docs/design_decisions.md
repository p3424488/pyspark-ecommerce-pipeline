# Design Decisions

## 1. Architecture

The project follows a modular PySpark data pipeline:

1. **Data generation:** Generates reproducible synthetic customer, product, and order datasets using a fixed random seed.
2. **Ingestion:** Reads CSV files using explicitly defined schemas.
3. **Validation:** Checks required fields, duplicate order IDs, quantity and price constraints, timestamps, and customer/product references.
4. **Transformation:** Enriches valid orders with customer and product information and calculates order revenue.
5. **Optimization:** Compares a baseline implementation with an optimized implementation.
6. **Publishing:** Writes transformed data and rejected records to Parquet.
7. **Monitoring:** Captures execution metadata, record counts, duration, status, and errors.

## 2. Technology Choices

- **Python:** Pipeline implementation and synthetic data generation.
- **PySpark:** Distributed data processing and transformations.
- **CSV:** Simple source data format.
- **Parquet:** Columnar output format suitable for analytical workloads.
- **Pytest:** Automated testing of validation, ingestion, transformations, edge cases, and publishing.

## 3. Data Quality Decisions

- Explicit schemas are used instead of relying on schema inference.
- Duplicate order IDs are rejected to protect the uniqueness of the order key.
- Invalid quantities and non-positive prices are rejected.
- Orders referencing nonexistent customers or products are rejected.
- Late-arriving records are identified separately and retained if they pass other validation rules.
- Rejected records are published separately for investigation.

## 4. Performance Optimization Decisions

The optimized transformation uses:

- Column pruning to reduce unnecessary data.
- Broadcast hints for smaller customer and product dimension datasets.
- Early selection of required columns.
- Parquet output partitioned by order month.

The baseline and optimized implementations are compared using execution time and execution plans. Spark may automatically optimize the baseline, so explicit broadcast hints are not assumed to guarantee faster execution.

## 5. Reliability and Rerun Strategy

- Each execution receives a unique run ID.
- Output publishing uses overwrite mode to produce a fresh snapshot rather than append duplicate records.
- Pipeline execution metadata is stored as a JSON report.
- Rejected records are kept separately from accepted output.

**Limitation:** Local overwrite-based publishing is not a transactional replacement strategy. It does not guarantee rollback to the previous snapshot if a write fails midway.

## 6. Scalability Considerations

The synthetic data generator supports configurable order volumes. The pipeline can therefore be tested with larger datasets without manually preparing source files.

For production-scale deployment, additional considerations would include transactional storage, incremental processing, orchestration, and distributed cluster configuration.