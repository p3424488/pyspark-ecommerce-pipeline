
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# --------------------------------------------------
# 1. CREATE ENRICHED ORDER DATASET
# --------------------------------------------------

def transform_orders(
    valid_orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame
) -> DataFrame:

    """
    Enrich valid orders with customer and product details.
    Calculate order revenue and derive reporting columns.
    """

    # Select only the required customer columns
    customers_selected = customers_df.select(
        "customer_id",
        "customer_name",
        "city"
    )

    # Select only the required product columns
    products_selected = products_df.select(
        "product_id",
        "product_name",
        "category"
    )

    # Join orders with customers
    enriched_df = valid_orders_df.join(
        customers_selected,
        on="customer_id",
        how="inner"
    )

    # Join the result with products
    enriched_df = enriched_df.join(
        products_selected,
        on="product_id",
        how="inner"
    )

    # Calculate order revenue
    enriched_df = enriched_df.withColumn(
        "order_revenue",
        (
            F.col("quantity").cast("decimal(12,2)")
            * F.col("unit_price")
        ).cast("decimal(20,2)")
    )

    # Derive order date and month
    enriched_df = (
        enriched_df
        .withColumn(
            "order_date",
            F.to_date("order_timestamp")
        )
        .withColumn(
            "order_month",
            F.date_format("order_timestamp", "yyyy-MM")
        )
    )

    return enriched_df


# --------------------------------------------------
# 2. REVENUE BY PRODUCT CATEGORY
# --------------------------------------------------

def revenue_by_category(enriched_df: DataFrame) -> DataFrame:

    return (
        enriched_df
        .groupBy("category")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("order_revenue").alias("total_revenue")
        )
        .orderBy(F.desc("total_revenue"))
    )


# --------------------------------------------------
# 3. REVENUE BY CITY
# --------------------------------------------------

def revenue_by_city(enriched_df: DataFrame) -> DataFrame:

    return (
        enriched_df
        .groupBy("city")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_revenue").alias("total_revenue")
        )
        .orderBy(F.desc("total_revenue"))
    )


# --------------------------------------------------
# 4. MONTHLY REVENUE
# --------------------------------------------------

def monthly_revenue(enriched_df: DataFrame) -> DataFrame:

    return (
        enriched_df
        .groupBy("order_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_revenue").alias("total_revenue")
        )
        .orderBy("order_month")
    )


# --------------------------------------------------
# 5. TOP CUSTOMERS
# --------------------------------------------------

def top_customers(
    enriched_df: DataFrame,
    limit: int = 10
) -> DataFrame:

    return (
        enriched_df
        .groupBy(
            "customer_id",
            "customer_name",
            "city"
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_revenue").alias("total_revenue")
        )
        .orderBy(F.desc("total_revenue"))
        .limit(limit)
    )
