from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_dim_product(sales_details_df: DataFrame) -> DataFrame:
    """Create dim_product from sales_details."""
    return sales_details_df.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("description"),
        F.col("category")
    ).distinct()

def create_dim_store(sales_df: DataFrame) -> DataFrame:
    """Create dim_store from sales."""
    return sales_df.select(
        F.col("store_id"),
        F.col("store_name"),
        F.col("address"),
        F.col("town"),
        F.col("number_of_employees")
    ).distinct()

def create_dim_date(sales_df: DataFrame) -> DataFrame:
    """Create dim_date from sales."""
    return sales_df.select(
        F.to_date(F.col("timestamp")).alias("date")
    ).distinct() \
        .withColumn(
            "date_id",
            (F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date")).cast("int")
        ) \
        .withColumn("day", F.dayofmonth("date")) \
        .withColumn("month", F.month("date")) \
        .withColumn("quarter", F.quarter("date")) \
        .withColumn("year", F.year("date")) \
        .select("date_id", "date", "day", "month", "quarter", "year")

#def create_dim_payment_method(payments_df: DataFrame, existing_dim: DataFrame = None) -> DataFrame:
#    """Create dim_payment_method from payments, handling new method IDs."""
#    new_methods = payments_df.select(F.col("method")).distinct()
#    if existing_dim is not None:
#        new_methods = new_methods.join(existing_dim.select("method"), on="method", how="left_anti")
#        max_id = existing_dim.agg(F.max("method_id")).collect()[0][0] or 0
#    else:
#        max_id = 0
#    return new_methods.withColumn(
#        "method_id", F.monotonically_increasing_id() + max_id + 1
#    ).withColumn(
#        "description", F.concat(F.lit("Payment via "), F.col("method"))
#    ).select("method_id", "method", "description")

#def create_fact_sales(sales_df: DataFrame, sales_details_df: DataFrame, payments_df: DataFrame, dim_date: DataFrame, dim_payment_method: DataFrame) -> DataFrame:
#    """Create fact_sales by joining cleaned data and dimensions."""
#    sales_alias = sales_df.select(
#        F.col("sale_id"),
#        F.col("store_id"),
#        F.col("total_amount"),
#        F.to_date(F.col("timestamp")).alias("sale_date")
#    ).alias("s")
#    details_alias = sales_details_df.alias("d")
#    payments_alias = payments_df.select(F.col("sale_id"), F.col("method")).alias("p")
#    date_alias = dim_date.alias("dt")
#    payment_dim_alias = dim_payment_method.alias("pm")
#    
#    return details_alias.join(
#        sales_alias, on="sale_id", how="inner"
#    ).join(
#        payments_alias, on="sale_id", how="inner"
#    ).join(
#        date_alias, F.col("s.sale_date") == F.col("dt.date"), "inner"
#    ).join(
#        payment_dim_alias, F.col("p.method") == F.col("pm.method"), "inner"
#    ).select(
#        F.col("d.sale_id"),
#        F.col("d.product_id"),
#        F.col("s.store_id"),
#        F.col("dt.date_id"),
#        F.col("pm.method_id"),
#        F.col("d.quantity"),
#        F.col("d.unit_price"),
#        (F.col("d.unit_price") * F.col("d.quantity")).alias("line_amount"),
#        F.col("d.VAT"),
#        F.col("s.sale_date").alias("date")
#    )
#


def create_fact_sales(sales_df: DataFrame, sales_details_df: DataFrame, payments_df: DataFrame, dim_date: DataFrame) -> DataFrame:
    """Create fact_sales by joining cleaned data and dimensions."""
    sales_alias = sales_df.select(
        F.col("sale_id"),
        F.col("store_id"),
        F.col("total_amount"),
        F.to_date(F.col("timestamp")).alias("sale_date")
    ).alias("s")
    
    details_alias = sales_details_df.alias("d")
    payments_alias = payments_df.select(F.col("sale_id"), F.col("method")).alias("p")
    date_alias = dim_date.alias("dt")
    
    return details_alias.join(
        sales_alias, on="sale_id", how="inner"
    ).join(
        payments_alias, on="sale_id", how="inner"
    ).join(
        date_alias, F.col("s.sale_date") == F.col("dt.date"), "inner"
    ).select(
        F.col("d.sale_id"),
        F.col("d.product_id"),
        F.col("s.store_id"),
        F.col("dt.date_id"),
        F.col("p.method").alias("payment_method"),  # Keep method as direct column
        F.col("d.quantity"),
        F.col("d.unit_price"),
        (F.col("d.unit_price") * F.col("d.quantity")).alias("line_amount"),
        F.col("d.VAT"),
        F.col("s.sale_date").alias("date")
    )