
import csv
import random
import argparse

from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

CITIES = [
    "Visakhapatnam",
    "Hyderabad",
    "Bengaluru",
    "Chennai",
    "Mumbai",
    "Delhi",
    "Pune",
    "Kolkata",
]

FIRST_NAMES = [
    "Rahul", "Pavan", "Arjun", "Priya", "Sneha",
    "Kiran", "Ananya", "Rohit", "Suresh", "Divya",
]

LAST_NAMES = [
    "Kumar", "Sharma", "Reddy", "Verma", "Rao",
    "Patel", "Singh", "Naidu", "Das", "Mehta",
]

PRODUCT_TYPES = [
    ("Laptop", "Electronics", 50000),
    ("Mouse", "Electronics", 800),
    ("Keyboard", "Electronics", 1500),
    ("Monitor", "Electronics", 12000),
    ("Phone", "Electronics", 25000),
    ("Chair", "Furniture", 4500),
    ("Table", "Furniture", 7000),
    ("Notebook", "Stationery", 100),
    ("Pen", "Stationery", 20),
    ("Bottle", "Accessories", 500),
]


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def random_date(start_date, end_date):
    """Generate a random date between two dates."""

    days = (end_date - start_date).days

    return start_date + timedelta(
        days=random.randint(0, days)
    )


def write_csv(file_path, fieldnames, rows):
    """Write a list of dictionaries into a CSV file."""

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        file_path,
        mode="w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(rows)


# --------------------------------------------------
# MAIN DATA GENERATOR
# --------------------------------------------------

def generate_dataset(
    output_dir,
    order_count=100000,
    seed=42
):

    random.seed(seed)

    output_dir = Path(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    print("\nStarting e-commerce data generation...")
    print(f"Output directory: {output_dir}")
    print(f"Requested normal orders: {order_count}")
    print(f"Random seed: {seed}")


    # --------------------------------------------------
    # 1. GENERATE CUSTOMERS
    # --------------------------------------------------

    print("\nGenerating customers...")

    customers = []

    # Keep registrations before all normal order dates.
    registration_start = datetime(2022, 1, 1).date()
    registration_end = datetime(2023, 12, 31).date()

    for i in range(1, 5001):

        customer = {
            "customer_id": f"CUS{i:05d}",

            "customer_name": (
                f"{random.choice(FIRST_NAMES)} "
                f"{random.choice(LAST_NAMES)}"
            ),

            "city": random.choice(CITIES),

            "registration_date": random_date(
                registration_start,
                registration_end
            ).isoformat(),
        }

        customers.append(customer)


    customer_ids = [
        customer["customer_id"]
        for customer in customers
    ]


    # --------------------------------------------------
    # 2. GENERATE PRODUCTS
    # --------------------------------------------------

    print("Generating products...")

    products = []

    for i in range(1, 501):

        product_name, category, base_price = random.choice(
            PRODUCT_TYPES
        )

        product = {
            "product_id": f"PRD{i:04d}",

            "product_name": f"{product_name} Model {i}",

            "category": category,

            "product_price": str(
                Decimal(str(base_price)).quantize(
                    Decimal("0.01")
                )
            ),
        }

        products.append(product)


    product_lookup = {
        product["product_id"]: Decimal(
            product["product_price"]
        )
        for product in products
    }

    product_ids = list(product_lookup.keys())


    # --------------------------------------------------
    # 3. GENERATE NORMAL ORDERS
    # --------------------------------------------------

    print(f"Generating {order_count} normal orders...")

    orders = []

    order_start = datetime(2024, 1, 1)
    order_end = datetime(2026, 10, 31, 23, 59, 59)

    ingestion_delays = [
        0, 0, 0, 1, 2, 5, 10, 30
    ]

    for i in range(1, order_count + 1):

        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)

        quantity = random.randint(1, 10)

        order_time = order_start + timedelta(
            seconds=random.randint(
                0,
                int((order_end - order_start).total_seconds())
            )
        )

        delay_days = random.choice(ingestion_delays)

        ingestion_time = order_time + timedelta(
            days=delay_days
        )

        # Selling price can differ from product list price
        # because of a random discount between 0% and 20%.

        base_price = product_lookup[product_id]

        discount = (
            Decimal(random.randint(0, 20))
            / Decimal("100")
        )

        unit_price = (
            base_price * (Decimal("1") - discount)
        ).quantize(Decimal("0.01"))

        order = {
            "order_id": f"ORD{i:07d}",

            "customer_id": customer_id,

            "product_id": product_id,

            "quantity": quantity,

            "unit_price": str(unit_price),

            "order_timestamp": order_time.isoformat(
                sep=" "
            ),

            "ingestion_timestamp": ingestion_time.isoformat(
                sep=" "
            ),
        }

        orders.append(order)


    # --------------------------------------------------
    # 4. ANOMALY HELPER
    # --------------------------------------------------

    def anomaly(anomaly_id, **overrides):
        """
        Create an anomaly row.

        anomaly_id is separate from order_id.
        This allows order_id itself to be empty
        without passing the same argument twice.
        """

        row = {
            "order_id": anomaly_id,

            "customer_id": random.choice(customer_ids),

            "product_id": random.choice(product_ids),

            "quantity": random.randint(1, 10),

            "unit_price": "500.00",

            "order_timestamp": "2026-01-15 10:00:00",

            "ingestion_timestamp": "2026-01-15 10:05:00",
        }

        row.update(overrides)

        return row


    # --------------------------------------------------
    # 5. INJECT NINE ANOMALIES
    # --------------------------------------------------

    print("Injecting intentional anomaly records...")

    # 1. Missing customer ID
    orders.append(
        anomaly(
            "ORD_INVALID_001",
            customer_id=""
        )
    )

    # 2. Invalid customer foreign key
    orders.append(
        anomaly(
            "ORD_INVALID_002",
            customer_id="CUS99999"
        )
    )

    # 3. Invalid product foreign key
    orders.append(
        anomaly(
            "ORD_INVALID_003",
            product_id="PRD9999"
        )
    )

    # 4. Negative quantity
    orders.append(
        anomaly(
            "ORD_INVALID_004",
            quantity=-5
        )
    )

    # 5. Zero selling price
    orders.append(
        anomaly(
            "ORD_INVALID_005",
            unit_price="0.00"
        )
    )

    # 6. Missing order ID
    orders.append(
        anomaly(
            "ORD_INVALID_006",
            order_id=""
        )
    )

    # 7. Missing order timestamp
    orders.append(
        anomaly(
            "ORD_INVALID_007",
            order_timestamp=""
        )
    )

    # 8. Duplicate order ID and record
    duplicate_order = orders[0].copy()
    orders.append(duplicate_order)

    # 9. Late-arriving event
    orders.append(
        anomaly(
            "ORD_INVALID_009",
            order_timestamp="2024-01-01 08:00:00",
            ingestion_timestamp="2026-10-31 12:00:00"
        )
    )


    # --------------------------------------------------
    # 6. WRITE CUSTOMER CSV
    # --------------------------------------------------

    customer_fields = [
        "customer_id",
        "customer_name",
        "city",
        "registration_date",
    ]

    write_csv(
        output_dir / "customers.csv",
        customer_fields,
        customers
    )


    # --------------------------------------------------
    # 7. WRITE PRODUCT CSV
    # --------------------------------------------------

    product_fields = [
        "product_id",
        "product_name",
        "category",
        "product_price",
    ]

    write_csv(
        output_dir / "products.csv",
        product_fields,
        products
    )


    # --------------------------------------------------
    # 8. WRITE ORDER CSV
    # --------------------------------------------------

    order_fields = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_timestamp",
        "ingestion_timestamp",
    ]

    write_csv(
        output_dir / "orders.csv",
        order_fields,
        orders
    )


    # --------------------------------------------------
    # 9. GENERATE MALFORMED CSV
    # --------------------------------------------------

    malformed_file = output_dir / "malformed_orders.csv"

    with open(
        malformed_file,
        mode="w",
        newline="",
        encoding="utf-8"
    ) as file:

        file.write(
            "order_id,customer_id,product_id,quantity,unit_price\n"
        )

        # Valid row
        file.write(
            "ORD_M001,CUS00001,PRD0001,2,45000.00\n"
        )

        # Missing fields
        file.write(
            "ORD_M002,CUS00002,PRD0002,3\n"
        )

        # Invalid integer quantity
        file.write(
            "ORD_M003,CUS00003,PRD0003,invalid,1000.00\n"
        )


    # --------------------------------------------------
    # 10. SUMMARY
    # --------------------------------------------------

    print("\n======================================")
    print("DATA GENERATION COMPLETED")
    print("======================================")

    print(f"Customers generated: {len(customers)}")
    print(f"Products generated: {len(products)}")
    print(f"Normal orders requested: {order_count}")
    print(f"Total orders generated: {len(orders)}")
    print("Intentional anomaly records: 9")
    print("Malformed CSV records: 2")
    print(f"Output location: {output_dir.resolve()}")

    print("======================================\n")


# --------------------------------------------------
# COMMAND LINE ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="E-commerce synthetic data generator"
    )

    parser.add_argument(
        "--output-dir",
        default="data/sample",
        help="Directory to save generated CSV files"
    )

    parser.add_argument(
        "--orders",
        type=int,
        default=100000,
        help="Number of normal orders to generate"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility"
    )

    args = parser.parse_args()

    generate_dataset(
        output_dir=args.output_dir,
        order_count=args.orders,
        seed=args.seed
    )