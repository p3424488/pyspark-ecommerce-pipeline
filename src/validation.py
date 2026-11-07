
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# --------------------------------------------------
# VALIDATION CONFIGURATION
# --------------------------------------------------

LATE_ARRIVAL_THRESHOLD_DAYS = 7


# --------------------------------------------------
# MAIN VALIDATION FUNCTION
# --------------------------------------------------

def validate_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame
):
    """
    Validate e-commerce orders.

    Returns:
        valid_orders_df
        rejected_orders_df
        late_orders_df
    """

    # --------------------------------------------------
    # 1. PREPARE REFERENCE DATA
    # --------------------------------------------------

    customer_ids = customers_df.select(
        "customer_id"
    ).distinct()

    product_ids = products_df.select(
        "product_id"
    ).distinct()

    # --------------------------------------------------
    # 2. IDENTIFY DUPLICATE ORDER IDS
    # --------------------------------------------------

    duplicate_ids = (
        orders_df
        .filter(
            F.col("order_id").isNotNull()
            & (F.trim(F.col("order_id")) != "")
        )
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id")
        .withColumn("is_duplicate", F.lit(True))
    )

    # --------------------------------------------------
    # 3. ADD VALIDATION FLAGS
    # --------------------------------------------------

    
    # --------------------------------------------------
    # 3. ADD VALIDATION FLAGS
    # --------------------------------------------------

    checked_df = (
        orders_df
        .join(
            duplicate_ids,
            on="order_id",
            how="left"
        )
        .withColumn(
            "is_duplicate",
            F.coalesce(
                F.col("is_duplicate"),
                F.lit(False)
            )
        )

        .withColumn(
            "has_missing_required_field",
            F.col("order_id").isNull()
            | (F.trim(F.col("order_id")) == "")
            | F.col("customer_id").isNull()
            | (F.trim(F.col("customer_id")) == "")
            | F.col("product_id").isNull()
            | (F.trim(F.col("product_id")) == "")
            | F.col("quantity").isNull()
            | F.col("unit_price").isNull()
            | F.col("order_timestamp").isNull()
            | F.col("ingestion_timestamp").isNull()
        )

        .withColumn(
            "invalid_quantity",
            F.col("quantity") <= 0
        )

        .withColumn(
            "invalid_price",
            F.col("unit_price") <= 0
        )

        .withColumn(
            "invalid_timestamp_order",
            F.col("ingestion_timestamp")
            < F.col("order_timestamp")
        )
    )

    # --------------------------------------------------
    # 4. CHECK CUSTOMER FOREIGN KEY
    # --------------------------------------------------

    checked_df = (
        checked_df
        .join(
            customer_ids.withColumn(
                "customer_exists",
                F.lit(True)
            ),
            on="customer_id",
            how="left"
        )
        .withColumn(
            "invalid_customer",
            F.coalesce(
                ~F.col("customer_exists"),
                F.lit(True)
            )
        )
        .drop("customer_exists")
    )

    # --------------------------------------------------
    # 5. CHECK PRODUCT FOREIGN KEY
    # --------------------------------------------------

    checked_df = (
        checked_df
        .join(
            product_ids.withColumn(
                "product_exists",
                F.lit(True)
            ),
            on="product_id",
            how="left"
        )
        .withColumn(
            "invalid_product",
            F.coalesce(
                ~F.col("product_exists"),
                F.lit(True)
            )
        )
        .drop("product_exists")
    )

    # --------------------------------------------------
    # 6. BUILD REJECTION REASONS
    # --------------------------------------------------

    rejection_conditions = [
        (
            F.col("has_missing_required_field"),
            "MISSING_REQUIRED_FIELD"
        ),
        (
            F.col("is_duplicate"),
            "DUPLICATE_ORDER_ID"
        ),
        (
            F.col("invalid_customer"),
            "INVALID_CUSTOMER_REFERENCE"
        ),
        (
            F.col("invalid_product"),
            "INVALID_PRODUCT_REFERENCE"
        ),
        (
            F.col("invalid_quantity"),
            "INVALID_QUANTITY"
        ),
        (
            F.col("invalid_price"),
            "INVALID_UNIT_PRICE"
        ),
        (
            F.col("invalid_timestamp_order"),
            "INGESTION_BEFORE_EVENT_TIME"
        ),
    ]

    rejection_reason = F.concat_ws(
        "|",
        *[
            F.when(condition, reason)
            for condition, reason in rejection_conditions
        ]
    )

    checked_df = checked_df.withColumn(
        "rejection_reason",
        rejection_reason
    )

    # --------------------------------------------------
    # 7. SEPARATE VALID AND REJECTED ORDERS
    # --------------------------------------------------

    rejected_orders_df = (
        checked_df
        .filter(F.col("rejection_reason") != "")
    )

    valid_orders_df = (
        checked_df
        .filter(F.col("rejection_reason") == "")
    )

    # --------------------------------------------------
    # 8. FLAG LATE-ARRIVING EVENTS
    # --------------------------------------------------

    valid_orders_df = (
        valid_orders_df
        .withColumn(
            "delay_days",
            F.datediff(
                F.to_date("ingestion_timestamp"),
                F.to_date("order_timestamp")
            )
        )
        .withColumn(
            "is_late_arriving",
            F.col("delay_days")
            > LATE_ARRIVAL_THRESHOLD_DAYS
        )
    )

    late_orders_df = valid_orders_df.filter(
        F.col("is_late_arriving")
    )

    return (
        valid_orders_df,
        rejected_orders_df,
        late_orders_df
    )
