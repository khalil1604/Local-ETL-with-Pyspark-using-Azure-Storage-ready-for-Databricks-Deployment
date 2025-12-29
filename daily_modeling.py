from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DailyModeling") \
    .config("spark.jars.packages", 
            "org.postgresql:postgresql:42.7.4,"
            "org.apache.hadoop:hadoop-azure:3.3.6,"
            "org.apache.hadoop:hadoop-azure-datalake:3.3.6") \
    .getOrCreate()

# ADLS Gen2 configuration
storage_account_name = "firstprojectde"
storage_account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = "steps"

# Set Hadoop configuration for ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Base ADLS path
adls_base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# Previous day’s date (e.g., 2025-08-27 on August 28)
yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

#test
yesterday = "2025-09-04"

# Read cleaned transactional data (previous day)
sales_df = spark.read.parquet(adls_base_path + f"cleaned/transactions/sales/date={yesterday}/") \
    .dropDuplicates(["sale_id"])
sales_details_df = spark.read.parquet(adls_base_path + f"cleaned/transactions/sales_details/date={yesterday}/") \
    .dropDuplicates(["detail_id"])
payments_df = spark.read.parquet(adls_base_path + f"cleaned/transactions/payments/date={yesterday}/") \
    .dropDuplicates(["payment_id"])

# Read existing dimension tables (for upsert)
dim_product_existing = spark.read.parquet(adls_base_path + "curated/analytics/dim_product/")
dim_store_existing = spark.read.parquet(adls_base_path + "curated/analytics/dim_store/")
dim_date_existing = spark.read.parquet(adls_base_path + "curated/analytics/dim_date/")
dim_payment_method_existing = spark.read.parquet(adls_base_path + "curated/analytics/dim_payment_method/")

  
# Create new dimension data from yesterday’s data

dim_product_new = sales_details_df.select(
    "product_id", "product_name", "description", "category"
).distinct()

dim_store_new = sales_df.select(
    "store_id", "store_name", "address", "town", "number_of_employees"
).distinct()

dim_date_new = sales_df.select(
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


# Find new methods that don’t exist in the current dimension
new_methods = payments_df.select("method").distinct() \
    .join(dim_payment_method_existing.select("method"), on="method", how="left_anti")

# Get current max_id from existing table
max_id = dim_payment_method_existing.agg(F.max("method_id")).collect()[0][0]
if max_id is None:
    max_id = 0

# Assign new IDs only to unseen methods
dim_payment_method_new = new_methods.withColumn(
    "method_id", F.monotonically_increasing_id() + max_id + 1
).withColumn(
    "description", F.concat(F.lit("Payment via "), F.col("method"))
)


# Merge dimensions (upsert: keep existing + add new)

dim_product = dim_product_existing.unionByName(dim_product_new, allowMissingColumns=True).distinct()
dim_store = dim_store_existing.unionByName(dim_store_new, allowMissingColumns=True).distinct()
dim_date = dim_date_existing.unionByName(dim_date_new, allowMissingColumns=True).distinct()
dim_payment_method = dim_payment_method_existing.unionByName(dim_payment_method_new, allowMissingColumns=True).distinct()


# Build fact_sales

sales_alias = sales_df.select(
    F.col("sale_id"),
    F.col("store_id"),
    F.to_date(F.col("timestamp")).alias("sale_date")
).alias("s")
details_alias = sales_details_df.alias("d")
payments_alias = payments_df.select("sale_id", "method").alias("p")
date_alias = dim_date.alias("dt")
payment_dim_alias = dim_payment_method.alias("pm")

fact_sales = details_alias.join(
    sales_alias, on="sale_id", how="inner"
).join(
    payments_alias, on="sale_id", how="inner"
).join(
    date_alias, F.col("s.sale_date") == F.col("dt.date"), "inner"
).join(
    payment_dim_alias, F.col("p.method") == F.col("pm.method"), "inner"
).select(
    F.col("d.sale_id"),
    F.col("d.product_id"),
    F.col("s.store_id"),
    F.col("dt.date_id"),
    F.col("pm.method_id"),
    F.col("d.quantity"),
    F.col("d.unit_price"),
    (F.col("d.unit_price") * F.col("d.quantity")).alias("line_amount"),
    F.col("d.VAT"),
    F.col("s.sale_date").alias("date")
)


fact_sales.write.partitionBy("date").mode("overwrite").parquet(adls_base_path + f"curated/analytics/fact_sales/date={yesterday}/")
dim_product.write.mode("overwrite").parquet(adls_base_path + "curated/analytics/dim_product/")
dim_store.write.mode("overwrite").parquet(adls_base_path + "curated/analytics/dim_store/")
dim_date.write.mode("overwrite").parquet(adls_base_path + "curated/analytics/dim_date/")
dim_payment_method.write.mode("overwrite").parquet(adls_base_path + "curated/analytics/dim_payment_method/")

# Stop Spark session
spark.stop()
