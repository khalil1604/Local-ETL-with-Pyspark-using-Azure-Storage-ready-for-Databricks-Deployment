from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, Any
from delta.tables import DeltaTable
from ...utils.config import get_path




def write_parquet(df: DataFrame, config: dict, step: str, layer: str, table: str, 
                  mode: str = "overwrite", date: Optional[str] = None) -> None:
    """
    Write DataFrame to Parquet at a specified path.
    
    Args:
        df: DataFrame to write.
        config: Configuration dictionary.
        step: Pipeline step (e.g., 'cleaned').
        layer: Data layer (e.g., 'transactions').
        table: Table name (e.g., 'sales').
        mode (str): Write mode ('overwrite', 'append').
        date (str, optional): Specific date partition (e.g., '2025-09-04').
    """
    path = get_path(config, step, layer, table)
    if date:
        path += f"date={date}/"
        df.write.mode(mode).parquet(path)
    else:
        df.write.partitionBy("date").mode(mode).parquet(path)


def write_delta_full(spark: SparkSession, df: DataFrame, config: dict, step: str, layer: str, 
                     table: str, partition_by: Optional[str] = None):
    """
    Write DataFrame as Delta table in full load mode (overwrite).
    
    Args:
        spark: SparkSession object.
        df: DataFrame to write.
        config: Configuration dictionary.
        step: Pipeline step (e.g., 'curated').
        layer: Data layer (e.g., 'analytics').
        table: Table name (e.g., 'fact_sales', 'dim_product').
        partition_by (str, optional): Column to partition by.
    """
    path = get_path(config, step, layer, table)
    
    writer = df.write.format("delta").mode("overwrite")
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.save(path)


def write_delta_append(spark: SparkSession, df: DataFrame, config: dict, step: str, layer: str, 
                       table: str, partition_by: str = "date"):
    """
    Write daily fact data by appending to existing Delta table.
    
    Args:
        spark: SparkSession object.
        df: DataFrame with daily fact data.
        config: Configuration dictionary.
        step: Pipeline step (e.g., 'curated').
        layer: Data layer (e.g., 'analytics').
        table: Table name (e.g., 'fact_sales').
        partition_by (str): Column to partition by (default: "date").
    """
    path = get_path(config, step, layer, table)
    
    df.write.format("delta") \
        .mode("append") \
        .partitionBy(partition_by) \
        .save(path)


def write_delta_merge(spark: SparkSession, df: DataFrame, config: dict, step: str, layer: str, 
                      table: str, merge_key: str):
    """
    Write daily dimension data by merging with existing Delta table.
    
    Args:
        spark: SparkSession object.
        df: DataFrame with daily dimension data.
        config: Configuration dictionary.
        step: Pipeline step (e.g., 'curated').
        layer: Data layer (e.g., 'analytics').
        table: Table name (e.g., 'dim_product').
        merge_key (str): Column to use for matching records (e.g., 'product_id').
    """
    path = get_path(config, step, layer, table)
    
    # Check if Delta table exists
    try:
        delta_table = DeltaTable.forPath(spark, path)
        
        # Merge logic
        merge_condition = f"target.{merge_key} = source.{merge_key}"
        
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
    except Exception:
        # Table doesn't exist yet, create it
        df.write.format("delta").save(path)