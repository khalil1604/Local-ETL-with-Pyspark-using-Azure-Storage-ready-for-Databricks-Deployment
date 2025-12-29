from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as sum_, when, lit, coalesce, to_date, round

def clean_sales_details(sales_details_df: DataFrame, products_df: DataFrame, sales_df: DataFrame) -> DataFrame:
    """
    Clean sales_details data, join with products and sales, and add VAT.
    
    Args:
        sales_details_df: Raw sales_details DataFrame.
        products_df: Products reference DataFrame.
        sales_df: Sales DataFrame for date information.
    
    Returns:
        DataFrame: Cleaned sales_details with VAT and product details.
    """
    sd = sales_details_df.select(
        col("detail_id"),
        col("sale_id"),
        col("product_id"),
        col("quantity").alias("qty"),
        col("unit_price").alias("price")
    ).alias("sd")
    pr = products_df.alias("pr")
    median_price = sales_details_df.where(col("unit_price") > 0).approxQuantile("unit_price", [0.5], 0.05)[0]
    s = sales_df.select(col("sale_id"), to_date(col("timestamp")).alias("sale_date"))
    
    return sd.filter((col("qty") > 0) & (col("price") > 0)) \
        .join(pr, col("sd.product_id") == col("pr.product_id"), "left") \
        .join(s, "sale_id", "left") \
        .withColumn("unit_price", when(col("price") > 5 * median_price, lit(5 * median_price)).otherwise(col("price"))) \
        .withColumn("quantity", col("qty")) \
        .withColumn("VAT", round(col("unit_price") * col("quantity") * lit(0.2), 2)) \
        .withColumn("date", col("sale_date")) \
        .select(
            col("detail_id"),
            col("sale_id"),
            col("sd.product_id").alias("product_id"),
            col("quantity"),
            col("unit_price"),
            col("VAT"),
            col("date"),
            col("pr.name").alias("product_name"),
            col("pr.description"),
            col("pr.category")
        )

def clean_sales(sales_df: DataFrame, sales_details_cleaned: DataFrame) -> DataFrame:
    """
    Clean sales data, correcting total_amount using sales_details.
    
    Args:
        sales_df: Raw sales DataFrame.
        sales_details_cleaned: Cleaned sales_details DataFrame.
    
    Returns:
        DataFrame: Cleaned sales with corrected total_amount and date.
    """
    sales_details_sum = sales_details_cleaned.groupBy("sale_id").agg(
        sum_(col("unit_price") * col("quantity") + col("VAT")).alias("calculated_total")
    )
    return sales_df.join(sales_details_sum, "sale_id", "left") \
        .withColumn("total_amount", when(col("total_amount") == 0, coalesce(col("calculated_total"), lit(0.01))).otherwise(col("total_amount"))) \
        .drop("calculated_total") \
        .withColumn("date", to_date(col("timestamp")))

def clean_payments(payments_df: DataFrame, sales_cleaned: DataFrame) -> DataFrame:
    """
    Clean payments data, correcting method and adding date.
    
    Args:
        payments_df: Raw payments DataFrame.
        sales_cleaned: Cleaned sales DataFrame for joining.
    
    Returns:
        DataFrame: Cleaned payments with corrected method and date.
    """
    p = payments_df.alias("p")
    s = sales_cleaned.select(col("sale_id"), col("total_amount"), to_date(col("timestamp")).alias("sale_date")).alias("s")
    return p.join(s, col("p.sale_id") == col("s.sale_id"), "inner") \
        .withColumn("method", when(col("p.method") == "Unknown", lit("Card")).otherwise(col("p.method"))) \
        .withColumn("date", col("s.sale_date")) \
        .select(
            col("p.payment_id"),
            col("p.sale_id"),
            col("method"),
            col("p.amount"),
            col("date")
        )

def enrich_sales_with_stores(sales_cleaned: DataFrame, stores_df: DataFrame) -> DataFrame:
    """
    Enrich sales data with store information.
    
    Args:
        sales_cleaned: Cleaned sales DataFrame.
        stores_df: Stores reference DataFrame.
    
    Returns:
        DataFrame: Sales enriched with store details.
    """
    return sales_cleaned.join(stores_df, "store_id", "left") \
        .select(
            sales_cleaned["*"],
            col("name").alias("store_name"),
            col("address"),
            col("town"),
            col("number_of_employees")
        )