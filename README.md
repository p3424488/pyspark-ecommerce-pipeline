# E-commerce Data Engineering Pipeline using PySpark

## Project Overview

This project implements an end-to-end e-commerce data processing pipeline using **Apache PySpark**. It processes synthetic customer, product, and order datasets, validates data quality, enriches order records, calculates revenue metrics, and publishes analytical outputs in partitioned Parquet format.

The project also compares baseline and optimized Spark transformations to evaluate performance improvements through column pruning and broadcast joins.

## Key Features

- Synthetic dataset generation with a reproducible random seed.
- CSV ingestion using explicit PySpark schemas.
- Data-quality validation and rejection-reason tracking.
- Referential-integrity checks for customer and product records.
- Handling of duplicate, null, malformed, and invalid records.
- Late-arriving order identification.
- Revenue analysis by category, city, month, and customer.
- Baseline versus optimized transformation benchmarking.
- Broadcast joins and column pruning.
- Partitioned Parquet publishing.
- Pipeline execution monitoring and JSON reports.
- Automated tests using pytest.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Application development |
| Apache PySpark 3.5.6 | Distributed data processing |
| Java JDK 17 | Spark runtime |
| Parquet | Analytical data storage |
| CSV | Source data format |
| pytest 8.3.5 | Automated testing |
| Git | Version control |

## Project Architecture

```text
Synthetic Data Generator
          |
          v
     CSV Datasets
          |
          v
       Ingestion
          |
          v
   Data Validation
      /       \
     v         v
 Valid Data  Rejected Data
     |
     v
 Transformation
     |
     v
 Baseline and Optimized Processing
     |
     v
 Revenue Aggregations
     |
     v
 Partitioned Parquet Publishing
     |
     v
 Monitoring and JSON Reports
```

## Dataset Description

The project generates the following datasets:

| Dataset | Records | Description |
|---|---:|---|
| Customers | 5,000 | Customer information |
| Products | 500 | Product catalogue |
| Orders | 100,009 | Order transactions, including injected anomalies |

The generator uses a fixed random seed (`42`) to support reproducibility.

The order dataset contains 100,000 normal records and 9 deliberately injected anomaly records.

### Data Quality Results

| Metric | Records |
|---|---:|
| Raw orders | 100,009 |
| Valid orders | 100,000 |
| Rejected orders | 9 |
| Late-arriving valid orders | 25,118 |

Late-arriving orders are retained as valid records and flagged separately.

## Processing and Optimization

### Baseline Processing

The baseline implementation performs the enrichment joins and revenue calculations using standard DataFrame operations.

### Optimized Processing

The optimized implementation applies:

- Column pruning to reduce unnecessary data processing.
- Explicit broadcast hints for smaller customer and product dimension tables.
- Focused selection of required columns.
- Partitioned Parquet output by order month.

The project compares execution times using repeated measurements and reports the observed results.

**Note:** Spark may automatically broadcast small tables in the baseline implementation as well. Performance results therefore depend on Spark's execution plan and the local environment.

## Project Structure

```text
pyspark-ecommerce-pipeline/
|
|-- data/
|   |-- sample/
|       |-- customers.csv
|       |-- products.csv
|       |-- orders.csv
|       |-- malformed_orders.csv
|
|-- docs/
|-- prompt_history/
|
|-- src/
|   |-- config.py
|   |-- data_generator.py
|   |-- ingestion.py
|   |-- validation.py
|   |-- transformation.py
|   |-- optimization.py
|   |-- publishing.py
|   |-- monitoring.py
|   |-- spark_session.py
|
|-- tests/
|-- run.py
|-- requirements.txt
|-- README.md
|-- .gitignore
```

## Setup Instructions

### Prerequisites

- Python 3.11
- Java JDK 17
- Git
- Windows users running local Spark should configure Hadoop Windows utilities compatible with the Hadoop version bundled with their PySpark installation.

### 1. Clone the repository

```bash
git clone https://github.com/p3424488/pyspark-ecommerce-pipeline.git
cd pyspark-ecommerce-pipeline
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 4. Generate the dataset (optional)

The repository includes sample datasets. To regenerate them:

```bash
python -m src.data_generator --orders 100000 --seed 42
```

### 5. Run the pipeline

```bash
python run.py
```

The pipeline reads the source CSV files, validates and transforms the records, benchmarks the processing implementations, publishes outputs, and creates a run summary.

## Output

Generated outputs are stored under the `output/` directory:

```text
output/
|-- optimized/
|   |-- orders/
|       |-- order_month=YYYY-MM/
|
|-- quarantine/
|   |-- rejected_orders/
|
|-- reports/
    |-- pipeline_summary_<run_id>.json
```

The optimized orders and rejected records are published in Parquet format. The run summary records execution status and processing counts.

## Running Tests

Execute the complete test suite:

```bash
python -m pytest tests/ -v
```

The test suite covers:

- Source ingestion and schema verification.
- Valid and empty datasets.
- Duplicate order identifiers.
- Invalid customer and product references.
- Invalid quantities and prices.
- Null required fields.
- Timestamp-order validation.
- Late-arriving orders.
- Revenue calculations and aggregations.
- Idempotent snapshot publishing.

## Design Considerations and Limitations

- The project uses synthetic data rather than production customer or transaction records.
- Each order represents one product line for simplicity.
- Customer and product tables are treated as relatively small dimension tables.
- Local execution uses Spark's local master; results may differ on a distributed cluster.
- Snapshot publishing uses overwrite mode. It is not a transactional storage solution.
- Benchmark results are environment-dependent and should not be interpreted as universal performance guarantees.

## AI Assistance Disclosure

AI tools were used to assist with selected implementation, debugging, test design, and documentation tasks. The generated code and recommendations were reviewed, executed, and tested during development.

See `prompt_history/` for the relevant prompt history.

## Author

**Pavan Kumar**

Data Science | Python | PySpark | Data Engineering
