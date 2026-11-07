
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input directories
DATA_DIR = PROJECT_ROOT / "data" / "sample"

CUSTOMERS_FILE = DATA_DIR / "customers.csv"
PRODUCTS_FILE = DATA_DIR / "products.csv"
ORDERS_FILE = DATA_DIR / "orders.csv"
MALFORMED_ORDERS_FILE = DATA_DIR / "malformed_orders.csv"

# Output directories
OUTPUT_DIR = PROJECT_ROOT / "output"

BASELINE_OUTPUT = OUTPUT_DIR / "baseline"
OPTIMIZED_OUTPUT = OUTPUT_DIR / "optimized"
QUARANTINE_OUTPUT = OUTPUT_DIR / "quarantine"
REPORTS_OUTPUT = OUTPUT_DIR / "reports"

# Spark configuration
APP_NAME = "EcommerceDataPipeline"

# Run configuration
RANDOM_SEED = 42
EXPECTED_CUSTOMERS = 5000
EXPECTED_PRODUCTS = 500
EXPECTED_ORDERS = 100009
