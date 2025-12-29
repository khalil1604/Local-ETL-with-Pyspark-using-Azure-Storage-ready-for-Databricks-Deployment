from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from datetime import datetime, timedelta
from ..utils.config import get_config
from ..utils.logging import setup_logging
from .aggregation_utils import create_daily_store_sales, create_daily_product_sales, create_monthly_customer_metrics, create_store_performance_dashboard, create_product_sales_trends  
from ..io.readers.readers import read_delta
from ..io.writers.writers import write_delta_full, write_delta_append

class AggregationManager:
    """Class to handle full or daily aggregation tables."""
    
    def __init__(self, spark: SparkSession, date: str = None):
        self.spark = spark
        self.config = get_config()
        self.logger = setup_logging()
        #self.date = date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        #self.month = (datetime.now() - timedelta(days=1)).strftime("%Y-%m")
        self.date = "2025-10-20"
        self.month = "2025-10"
    def run_full_aggregation(self):
        """Execute full aggregation, creating all summary tables."""
        self.logger.info("Starting full aggregation")
        try:
            # Read modeled data from curated layer
            fact_sales = read_delta(self.spark, self.config, "curated", "analytics", "fact_sales")
            dim_store = read_delta(self.spark, self.config, "curated", "analytics", "dim_store")
            dim_product = read_delta(self.spark, self.config, "curated", "analytics", "dim_product")
            dim_date = read_delta(self.spark, self.config, "curated", "analytics", "dim_date")
            
            # Create aggregated tables
            daily_store_sales = create_daily_store_sales(fact_sales, dim_store)
            daily_product_sales = create_daily_product_sales(fact_sales, dim_product)
            monthly_customer_metrics = create_monthly_customer_metrics(fact_sales)

            store_performance = create_store_performance_dashboard(fact_sales, dim_store, dim_date)
            product_trends = create_product_sales_trends(fact_sales, dim_product, dim_date)

            
            # Write to serving layer
            write_delta_full(self.spark, daily_store_sales, self.config, "serving", "aggregated", "daily_store_sales", "date")
            write_delta_full(self.spark, daily_product_sales, self.config, "serving", "aggregated", "daily_product_sales", "date")
            write_delta_full(self.spark, product_trends, self.config, "serving", "aggregated", "product_sales_trends", "date")
            write_delta_full(self.spark, store_performance, self.config, "serving", "aggregated", "store_performance", "month_year")
            write_delta_full(self.spark, monthly_customer_metrics, self.config, "serving", "aggregated", "monthly_customer_metrics", "month_year")

            self.logger.info("Full aggregation completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during full aggregation: {str(e)}")
            raise
    
    def run_daily_aggregation(self):
        """Execute daily aggregation for incremental updates."""
        self.logger.info(f"Starting daily aggregation for date={self.date}")
        try:
            # Read only specific date from modeled data
            fact_sales = read_delta(self.spark, self.config, "curated", "analytics", "fact_sales").filter(F.col("date") == self.date)
            dim_store = read_delta(self.spark, self.config, "curated", "analytics", "dim_store")
            dim_product = read_delta(self.spark, self.config, "curated", "analytics", "dim_product")
            dim_date = read_delta(self.spark, self.config, "curated", "analytics", "dim_date")
            
            # Create daily aggregated tables
            daily_store_sales = create_daily_store_sales(fact_sales, dim_store)
            daily_product_sales = create_daily_product_sales(fact_sales, dim_product)
            
            product_trends = create_product_sales_trends(fact_sales, dim_product, dim_date)
            # Merge into existing aggregated tables
            write_delta_append(self.spark, daily_store_sales, self.config, "serving", "aggregated", "daily_store_sales", "date")
            write_delta_append(self.spark, daily_product_sales, self.config, "serving", "aggregated", "daily_product_sales", "date")
            write_delta_append(self.spark, product_trends, self.config, "serving", "aggregated", "product_sales_trends", "date")

            
            self.logger.info("Daily aggregation completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during daily aggregation: {str(e)}")
            raise
    

    def run_monthly_aggregation(self):
        """Execute daily aggregation for incremental updates."""
        self.logger.info(f"Starting daily aggregation for date={self.month}")
        try: 
            fact_sales = read_delta(self.spark, self.config, "curated", "analytics", "fact_sales").filter(F.date_format("date", "yyyy-MM") == self.month)
            dim_store = read_delta(self.spark, self.config, "curated", "analytics", "dim_store")
            dim_date = read_delta(self.spark, self.config, "curated", "analytics", "dim_date")

            monthly_customer_metrics = create_monthly_customer_metrics(fact_sales)
            store_performance = create_store_performance_dashboard(fact_sales, dim_store, dim_date)

            # Write to serving layer
            write_delta_full(self.spark, monthly_customer_metrics, self.config, "serving", "aggregated", "monthly_customer_metrics", "month_year")
            write_delta_full(self.spark, store_performance, self.config, "serving", "aggregated", "store_performance", "month_year")

            self.logger.info("Monthly aggregation completed successfully")

        except Exception as e:
            self.logger.error(f"Error during monthly aggregation: {str(e)}")
            raise





    def run(self, mode: str):
        """Run the specified aggregation mode."""
        self.logger.info(f"Executing aggregation in {mode} mode")
        if mode == "full":
            self.run_full_aggregation()
        elif mode == "daily":
            self.run_daily_aggregation()
        elif mode == "monthly":
            self.run_monthly_aggregation()

        else:
            raise ValueError("Mode must be 'full' or 'daily' or 'monthly'")