
# Data Dictionary

## Project
E-commerce Data Processing and Performance Optimization Using PySpark

## 1. Customers

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| customer_id | STRING | PK | No | Unique customer identifier |
| customer_name | STRING | | No | Customer full name |
| city | STRING | | No | Customer city |
| registration_date | DATE | | No | Customer registration date |

Grain: One row per customer.

## 2. Products

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| product_id | STRING | PK | No | Unique product identifier |
| product_name | STRING | | No | Product name |
| category | STRING | | No | Product category |
| product_price | DECIMAL(12,2) | | No | Listed product price |

Grain: One row per product.

## 3. Orders

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| order_id | STRING | PK | No | Unique transaction identifier |
| customer_id | STRING | FK | No | Reference to customers |
| product_id | STRING | FK | No | Reference to products |
| quantity | INTEGER | | No | Number of items |
| unit_price | DECIMAL(12,2) | | No | Actual selling price |
| order_timestamp | TIMESTAMP | | No | Time of purchase |
| ingestion_timestamp | TIMESTAMP | | No | Time received by pipeline |

Grain: One product line per order.

## Relationships

- One customer can place multiple orders.
- One product can appear in multiple orders.
- Every valid order must reference an existing customer and product.

## Business Rules

1. All primary keys must be unique and non-null.
2. All foreign keys must reference existing records.
3. Quantity must be a positive integer.
4. Unit price must be greater than zero.
5. Required timestamps must be valid.
6. Revenue = quantity * unit_price.
7. Invalid records must be quarantined with rejection reasons.
8. Duplicate order IDs must not create duplicate target records.

## Naming Conventions

- snake_case column names.
- Stable deterministic IDs.
- ISO-8601 timestamps.
- CSV for source files.
- Parquet for analytical outputs.
- run_id and source_file added during pipeline processing.
