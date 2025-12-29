from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from datetime import datetime
from ..utils.config import get_config, get_path
from ..utils.logging import setup_logging
from ..io.readers.readers import read_postgres_table, read_csv

class IngestionManager:
    """Class to handle full, daily, or monthly ingestion based on mode."""
    
    def __init__(self, spark: SparkSession, date_filter: str = "CURRENT_DATE - INTERVAL '1 day'", version: str = None):
        """
        Initialize ingestion manager.
        
        Args:
            spark: SparkSession object.
            date_filter (str): SQL filter for daily ingestion (e.g., 'CURRENT_DATE - INTERVAL ''1 day''').
            version (str): Version for monthly ingestion (e.g., '2025-09').
        """
        self.spark = spark
        self.config = get_config()
        self.logger = setup_logging()
        self.date_filter = date_filter
        
        self.version = version or datetime.now().strftime("%Y-%m")
    
    def run_full_ingestion(self):
        """Execute full ingestion of transactional and reference data."""
        self.logger.info("Starting full ingestion")
        try:
            # Read transactional data from PostgreSQL
            sales_df = read_postgres_table(
                self.spark, self.config["jdbc_url"], self.config["db_properties"], "sales"
            )
            sales_details_df = read_postgres_table(
                self.spark, self.config["jdbc_url"], self.config["db_properties"], "sales_details"
            )
            payments_df = read_postgres_table(
                self.spark, self.config["jdbc_url"], self.config["db_properties"], "payments"
            )
            self.logger.info("Loaded transactional data from PostgreSQL")
            
            # Add date column and join
            sales_df = sales_df.withColumn("date", to_date(col("timestamp")))
            sales_details_df = sales_details_df.join(sales_df.select("sale_id", "date"), "sale_id", "inner")
            payments_df = payments_df.join(sales_df.select("sale_id", "date"), "sale_id", "inner")
            self.logger.info("Completed joins for transactional data")
            
            # Write transactional data
            sales_df.write.partitionBy("date").mode("overwrite").parquet(get_path(self.config, "raw", "transactions", "sales"))
            sales_details_df.write.partitionBy("date").mode("overwrite").parquet(get_path(self.config, "raw", "transactions", "sales_details"))
            payments_df.write.partitionBy("date").mode("overwrite").parquet(get_path(self.config, "raw", "transactions", "payments"))
            self.logger.info("Wrote transactional data")
            
            # Read and write reference data
            products_df = read_csv(self.spark, "csv_data2/products.csv")
            stores_df = read_csv(self.spark, "csv_data2/stores.csv")
            self.logger.info("Loaded reference data")
            
            write_mode = "overwrite"
            products_df.write.mode(write_mode).parquet(get_path(self.config, "raw", "reference", "products", "2025-07-15"))
            stores_df.write.mode(write_mode).parquet(get_path(self.config, "raw", "reference", "stores", "2025-07-15"))
            self.logger.info("Wrote reference data")
            
            self.logger.info("Full ingestion completed successfully")
        except Exception as e:
            self.logger.error(f"Error during full ingestion: {str(e)}")
            raise
    
    def run_daily_ingestion(self):
        """Execute daily incremental ingestion of transactional data."""
        self.logger.info("Starting daily ingestion")
        try:
            # Read sales with incremental filter
            sales_query = f"(SELECT * FROM sales WHERE timestamp >= {self.date_filter}) AS sales"
            sales_df = read_postgres_table(
                self.spark, self.config["jdbc_url"], self.config["db_properties"], query=sales_query
            )
            sales_details_df = read_postgres_table(
                self.spark, self.config["jdbc_url"], self.config["db_properties"], "sales_details"
            )
            payments_df = read_postgres_table(
                self.spark, self.config["jdbc_url"], self.config["db_properties"], "payments"
            )
            self.logger.info("Loaded transactional data from PostgreSQL")
            
            # Add date column and join
            sales_df = sales_df.withColumn("date", to_date(col("timestamp")))
            sales_details_df = sales_details_df.join(sales_df.select("sale_id", "date"), "sale_id", "inner")
            payments_df = payments_df.join(sales_df.select("sale_id", "date"), "sale_id", "inner")
            self.logger.info("Completed joins for transactional data")
            
            # Write to daily partitions (append mode)
            sales_df.write.partitionBy("date").mode("append").parquet(get_path(self.config, "raw", "transactions", "sales"))
            sales_details_df.write.partitionBy("date").mode("append").parquet(get_path(self.config, "raw", "transactions", "sales_details"))
            payments_df.write.partitionBy("date").mode("append").parquet(get_path(self.config, "raw", "transactions", "payments"))
            self.logger.info("Wrote new transactional data")
            
            self.logger.info("Daily ingestion completed successfully")
        except Exception as e:
            self.logger.error(f"Error during daily ingestion: {str(e)}")
            raise
    
    def run_monthly_ingestion(self):
        """Execute monthly ingestion of reference data."""
        self.logger.info("Starting monthly ingestion")
        try:
            # Read reference data
            products_df = read_csv(self.spark, "csv_data2/products.csv")
            stores_df = read_csv(self.spark, "csv_data2/stores.csv")
            self.logger.info("Loaded reference data")
            
            # Write to versioned folders
            write_mode = "errorifexists"
            products_df.write.mode(write_mode).parquet(get_path(self.config, "raw", "reference", "products", self.version))
            stores_df.write.mode(write_mode).parquet(get_path(self.config, "raw", "reference", "stores", self.version))
            self.logger.info(f"Wrote reference data for version={self.version}")
            
            # Log counts for verification
            self.logger.info(f"Products count: {products_df.count()}")
            self.logger.info(f"Stores count: {stores_df.count()}")
            
            self.logger.info("Monthly ingestion completed successfully")
        except Exception as e:
            self.logger.error(f"Error during monthly ingestion: {str(e)}")
            raise
    
    def run(self, mode: str):
        """Run the specified ingestion mode."""
        self.logger.info(f"Executing ingestion in {mode} mode")
        mode = mode.lower()
        if mode == "full":
            self.run_full_ingestion()
        elif mode == "daily":
            self.run_daily_ingestion()
        elif mode == "monthly":
            self.run_monthly_ingestion()
        else:
            self.logger.error("Invalid ingestion mode")
            raise ValueError("Mode must be 'full', 'daily', or 'monthly'")