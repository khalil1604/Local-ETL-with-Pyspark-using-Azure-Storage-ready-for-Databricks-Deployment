from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
from ..utils.config import get_config
from ..utils.logging import setup_logging
from ..io.readers.readers import read_parquet
from ..io.writers.writers import write_delta_full, write_delta_append, write_delta_merge
from .modeling_utils import create_dim_product, create_dim_store, create_dim_date, create_fact_sales

class ModelingManager:
    """Class to handle full or daily modeling into fact and dimension Delta tables."""
    
    def __init__(self, spark: SparkSession, date: str = None):
        """
        Initialize modeling manager.
        
        Args:
            spark: SparkSession object.
            date (str, optional): Date for daily modeling (e.g., '2025-09-04').
        """
        self.spark = spark
        self.config = get_config()
        self.logger = setup_logging()
        #self.date = date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.date = "2025-10-20"
        self.version = datetime.now().strftime("%Y-%m")
    
    def run_full_modeling(self):
        """Execute full modeling, overwriting Delta tables."""
        self.logger.info("Starting full modeling")
        try:
            # Read cleaned transactional data
            sales_df = read_parquet(self.spark, self.config, "cleaned", "transactions", "sales").dropDuplicates(["sale_id"])
            sales_details_df = read_parquet(self.spark, self.config, "cleaned", "transactions", "sales_details").dropDuplicates(["detail_id"])
            payments_df = read_parquet(self.spark, self.config, "cleaned", "transactions", "payments").dropDuplicates(["payment_id"])
            self.logger.info("Loaded cleaned transactional data")
            
            # Create dimensions
            dim_product = create_dim_product(sales_details_df)
            dim_store = create_dim_store(sales_df)
            dim_date = create_dim_date(sales_df)
            #dim_payment_method = create_dim_payment_method(payments_df)
            self.logger.info("Created dimension tables")
            
            # Create fact table
            fact_sales = create_fact_sales(sales_df, sales_details_df, payments_df, dim_date)
            
            # Write to Delta tables (overwrite)
            write_delta_full(self.spark, dim_product, self.config, "curated", "analytics", "dim_product")
            write_delta_full(self.spark, dim_store, self.config, "curated", "analytics", "dim_store")
            write_delta_full(self.spark, dim_date, self.config, "curated", "analytics", "dim_date")
            #write_delta_full(self.spark, dim_payment_method, self.config, "curated", "analytics", "dim_payment_method")
            write_delta_full(self.spark, fact_sales, self.config, "curated", "analytics", "fact_sales", "date")
            self.logger.info("Wrote fact and dimension tables to Delta")
            
            self.logger.info("Full modeling completed successfully")
        except Exception as e:
            self.logger.error(f"Error during full modeling: {str(e)}")
            raise
    
    def run_daily_modeling(self):
        """Execute daily modeling, merging into Delta tables."""
        self.logger.info(f"Starting daily modeling for date={self.date}")
        try:
            # Read cleaned transactional data (specific date)
            sales_df = read_parquet(self.spark, self.config, "cleaned", "transactions", "sales", date=self.date).dropDuplicates(["sale_id"])
            sales_details_df = read_parquet(self.spark, self.config, "cleaned", "transactions", "sales_details", date=self.date).dropDuplicates(["detail_id"])
            payments_df = read_parquet(self.spark, self.config, "cleaned", "transactions", "payments", date=self.date).dropDuplicates(["payment_id"])
            self.logger.info("Loaded cleaned transactional data")
            
            # Read existing dimension tables for merge
            #dim_product_existing = read_parquet(self.spark, self.config, "curated", "analytics", "dim_product")
            #dim_store_existing = read_parquet(self.spark, self.config, "curated", "analytics", "dim_store")
            #dim_date_existing = read_parquet(self.spark, self.config, "curated", "analytics", "dim_date")
            #dim_payment_method_existing = read_parquet(self.spark, self.config, "curated", "analytics", "dim_payment_method")
            #self.logger.info("Loaded existing dimension tables")
            
            # Create new dimension data
            dim_product_new = create_dim_product(sales_details_df)
            dim_store_new = create_dim_store(sales_df)
            dim_date_new = create_dim_date(sales_df)
            self.logger.info("Created new dimension data")
            
            # Create fact table
            fact_sales = create_fact_sales(sales_df, sales_details_df, payments_df, dim_date_new)
            self.logger.info("Created fact_sales table")
            self.logger.info(f"Fact sales show : {fact_sales.show()}")
            self.logger.info(f"Fact sales row count: {fact_sales.count()}")
            self.logger.info(f"Fact sales dates: {fact_sales.select('date').distinct().collect()}")
            # Write to Delta tables (merge)
            write_delta_merge(self.spark, dim_product_new, self.config, "curated", "analytics", "dim_product", "product_id")
            write_delta_merge(self.spark, dim_store_new, self.config, "curated", "analytics", "dim_store", "store_id")
            write_delta_merge(self.spark, dim_date_new, self.config, "curated", "analytics", "dim_date", "date_id")
            #write_delta_merge(self.spark, dim_payment_method_new, self.config, "curated", "analytics", "dim_payment_method", "method_id")
            write_delta_append(self.spark, fact_sales, self.config, "curated", "analytics", "fact_sales", "date")
            self.logger.info(f"Wrote fact and dimension tables to Delta for date={self.date}")
            
            self.logger.info("Daily modeling completed successfully")
        except Exception as e:
            self.logger.error(f"Error during daily modeling: {str(e)}")
            raise
    
    def run(self, mode: str):
        """Run the specified modeling mode."""
        self.logger.info(f"Executing modeling in {mode} mode")
        mode = mode.lower()
        if mode == "full":
            self.run_full_modeling()
        elif mode == "daily":
            self.run_daily_modeling()
        else:
            self.logger.error("Invalid modeling mode")
            raise ValueError("Mode must be 'full' or 'daily'")