from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def create_daily_store_sales(fact_sales_df: DataFrame, dim_store_df: DataFrame) -> DataFrame:
    """Create daily sales summary by store with store details."""
    return fact_sales_df.alias("f") \
        .join(dim_store_df.alias("s"), "store_id") \
        .groupBy("f.date", "s.store_id", "s.store_name", "s.town") \
        .agg(
            F.sum("f.line_amount").alias("daily_sales"),
            F.sum("f.quantity").alias("total_items_sold"),
            F.count("f.sale_id").alias("transaction_count"),
            F.avg("f.line_amount").alias("avg_transaction_value")
        )

def create_daily_product_sales(fact_sales_df: DataFrame, dim_product_df: DataFrame) -> DataFrame:
    """Create daily sales summary by product with product details."""
    return fact_sales_df.alias("f") \
        .join(dim_product_df.alias("p"), "product_id") \
        .groupBy("f.date", "p.product_id", "p.product_name", "p.category") \
        .agg(
            F.sum("f.line_amount").alias("daily_revenue"),
            F.sum("f.quantity").alias("total_quantity_sold"),
            F.count("f.sale_id").alias("times_sold")
        )

def create_monthly_customer_metrics(fact_sales_df: DataFrame) -> DataFrame:
    """Create monthly customer performance metrics."""
    return fact_sales_df.withColumn("month", F.date_trunc("month", F.col("date"))) \
        .withColumn("month_partition", F.date_format("month", "yyyy-MM")) \
        .groupBy("month_partition") \
        .agg(
            F.countDistinct("sale_id").alias("total_transactions"),
            F.sum("line_amount").alias("monthly_revenue"),
            F.avg("line_amount").alias("avg_sale_amount")
        ) \
        .withColumnRenamed("month_partition", "month_year") 


#def create_store_performance_dashboard(fact_sales_df: DataFrame, dim_store_df: DataFrame, dim_date_df: DataFrame) -> DataFrame:
#    """Create comprehensive store performance metrics for dashboards."""
#    return fact_sales_df.alias("f") \
#        .join(dim_store_df.alias("s"), "store_id") \
#        .join(dim_date_df.alias("d"), "date_id") \
#        .groupBy("s.store_id", "s.store_name", "s.town", "d.year", "d.month") \
#        .agg(
#            F.sum("f.line_amount").alias("monthly_revenue"),
#            F.sum("f.quantity").alias("total_items_sold"),
#            F.count_distinct("f.sale_id").alias("transaction_count"),
#            F.avg("f.line_amount").alias("avg_transaction_value"),
#            F.count_distinct("f.date").alias("trading_days"),
#            (F.sum("f.line_amount") / F.count_distinct("f.date")).alias("avg_daily_revenue")
#        ) \
#        .orderBy("s.store_name", "d.year", "d.month")

def create_store_performance_dashboard(fact_sales_df: DataFrame, dim_store_df: DataFrame, dim_date_df: DataFrame) -> DataFrame:
    """Create comprehensive store performance metrics for dashboards."""
    joined_df = fact_sales_df.alias("f") \
        .join(dim_store_df.alias("s"), "store_id") \
        .join(dim_date_df.alias("d"), "date_id")
    
    result = joined_df.groupBy("s.store_id", "s.store_name", "s.town", "d.year", "d.month") \
        .agg(
            F.sum("f.line_amount").alias("monthly_revenue"),
            F.sum("f.quantity").alias("total_items_sold"),
            F.count_distinct("f.sale_id").alias("transaction_count"),
            F.avg("f.line_amount").alias("avg_transaction_value"),
            F.count_distinct("f.date").alias("trading_days")
        ) \
        .withColumn("month_year", F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month"), 2, "0")))  # Create clean month partition
    
    return result

def create_product_sales_trends(fact_sales_df: DataFrame, dim_product_df: DataFrame, dim_date_df: DataFrame) -> DataFrame:
    """Create product performance with trends for analysis."""
    return fact_sales_df.alias("f") \
        .join(dim_product_df.alias("p"), "product_id") \
        .join(dim_date_df.alias("d"), "date_id") \
        .groupBy("p.product_id", "p.product_name", "p.category", "d.year", "d.month", "d.date") \
        .agg(
            F.sum("f.line_amount").alias("daily_revenue"),
            F.sum("f.quantity").alias("daily_quantity_sold"),
            F.count("f.sale_id").alias("times_sold_today")
        )