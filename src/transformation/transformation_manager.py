from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
from ..utils.config import get_config
from ..utils.logging import setup_logging
from .cleaning_utils import clean_sales_details, clean_sales, clean_payments, enrich_sales_with_stores
from ..io.readers.readers import read_parquet
from ..io.writers.writers import write_parquet

class TransformationManager:
    """Class to handle full or daily transformation of transactional and reference data."""
    
    def __init__(self, spark: SparkSession, date: str = None, version: str = None):
        """
        Initialize transformation manager.
        
        Args:
            spark: SparkSession object.
            date (str, optional): Date for daily transformation (e.g., '2025-09-04').
            version (str, optional): Version for reference data (e.g., '2025-09').
        """
        self.spark = spark
        self.config = get_config()
        self.logger = setup_logging()
        self.date = "2025-10-20"
        #self.date = date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.version = version or datetime.now().strftime("%Y-%m")
    
    def run_full_transformation(self):
        """Execute full transformation of transactional and reference data."""
        self.logger.info("Starting full transformation")
        try:
            # Read raw transactional data
            sales_df = read_parquet(self.spark, self.config, "raw", "transactions", "sales").dropDuplicates(["sale_id"])
            sales_details_df = read_parquet(self.spark, self.config, "raw", "transactions", "sales_details").dropDuplicates(["detail_id"])
            payments_df = read_parquet(self.spark, self.config, "raw", "transactions", "payments").dropDuplicates(["payment_id"])
            self.logger.info("Loaded transactional data")
            
            # Read reference data
            products_df = read_parquet(self.spark, self.config, "raw", "reference", "products", version=self.version)
            stores_df = read_parquet(self.spark, self.config, "raw", "reference", "stores", version=self.version)
            self.logger.info("Loaded reference data")
            
            # Clean data
            sales_details_cleaned = clean_sales_details(sales_details_df, products_df, sales_df)
            self.logger.info("Cleaned sales_details data")
            sales_cleaned = clean_sales(sales_df, sales_details_cleaned)
            self.logger.info("Cleaned sales data")
            payments_cleaned = clean_payments(payments_df, sales_cleaned)
            self.logger.info("Cleaned payments data")
            
            # Enrich sales
            sales_enriched = enrich_sales_with_stores(sales_cleaned, stores_df)
            self.logger.info("Enriched sales data with store information")
            
            # Write cleaned and enriched data
            write_parquet(sales_enriched, self.config, "cleaned", "transactions", "sales", mode="overwrite")
            write_parquet(sales_details_cleaned, self.config, "cleaned", "transactions", "sales_details", mode="overwrite")
            write_parquet(payments_cleaned, self.config, "cleaned", "transactions", "payments", mode="overwrite")
            self.logger.info("Wrote cleaned and enriched data")
            
            self.logger.info("Full transformation completed successfully")
        except Exception as e:
            self.logger.error(f"Error during full transformation: {str(e)}")
            raise
    
    def run_daily_transformation(self):
        """Execute daily transformation of transactional data for a specific date."""
        self.logger.info(f"Starting daily transformation for date={self.date}")
        try:
            # Read raw transactional data (specific date partition)
            sales_df = read_parquet(self.spark, self.config, "raw", "transactions", "sales", date=self.date).dropDuplicates(["sale_id"])
            sales_details_df = read_parquet(self.spark, self.config, "raw", "transactions", "sales_details", date=self.date).dropDuplicates(["detail_id"])
            payments_df = read_parquet(self.spark, self.config, "raw", "transactions", "payments", date=self.date).dropDuplicates(["payment_id"])
            self.logger.info("Loaded transactional data")
            
            # Read reference data
            products_df = read_parquet(self.spark, self.config, "raw", "reference", "products", version=self.version)
            stores_df = read_parquet(self.spark, self.config, "raw", "reference", "stores", version=self.version)
            self.logger.info("Loaded reference data")
            
            # Clean data
            sales_details_cleaned = clean_sales_details(sales_details_df, products_df, sales_df)
            self.logger.info("Cleaned sales_details data")
            sales_cleaned = clean_sales(sales_df, sales_details_cleaned)
            self.logger.info("Cleaned sales data")
            payments_cleaned = clean_payments(payments_df, sales_cleaned)
            self.logger.info("Cleaned payments data")
            
            # Enrich sales
            sales_enriched = enrich_sales_with_stores(sales_cleaned, stores_df)
            self.logger.info("Enriched sales data with store information")
            
            # Write cleaned and enriched data
            write_parquet(sales_enriched, self.config, "cleaned", "transactions", "sales", mode="overwrite", date=self.date)
            write_parquet(sales_details_cleaned, self.config, "cleaned", "transactions", "sales_details", mode="overwrite", date=self.date)
            write_parquet(payments_cleaned, self.config, "cleaned", "transactions", "payments", mode="overwrite", date=self.date)
            self.logger.info(f"Wrote cleaned and enriched data for date={self.date}")
            
            self.logger.info("Daily transformation completed successfully")
        except Exception as e:
            self.logger.error(f"Error during daily transformation: {str(e)}")
            raise
    
    def run(self, mode: str):
        """Run the specified transformation mode."""
        self.logger.info(f"Executing transformation in {mode} mode")
        mode = mode.lower()
        if mode == "full":
            self.run_full_transformation()
        elif mode == "daily":
            self.run_daily_transformation()
        else:
            self.logger.error("Invalid transformation mode")
            raise ValueError("Mode must be 'full' or 'daily'")