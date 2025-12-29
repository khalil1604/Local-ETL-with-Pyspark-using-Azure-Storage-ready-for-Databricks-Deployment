from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, Any
from delta.tables import DeltaTable
from ...utils.config import get_path


def read_csv(spark: SparkSession, file_path: str):
    """
    Read a CSV file into a DataFrame.
    
    Args:
        spark: SparkSession object.
        file_path (str): Path to CSV file.
    
    Returns:
        DataFrame: Data from CSV file.
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)


def read_postgres_table(spark: SparkSession, jdbc_url: str, properties: dict, table: Optional[str] = None,
                        query: Optional[str] = None):
    """
    Read a table or query from PostgreSQL.
    
    Args:
        spark: SparkSession object.
        jdbc_url (str): JDBC URL for PostgreSQL.
        properties (dict): JDBC connection properties.
        table (str): Table name to read.
        query (str, optional): Custom SQL query for filtered read.
    
    Returns:
        DataFrame: Data from PostgreSQL.
    """
    if query:
        return spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    return spark.read.jdbc(url=jdbc_url, table=table, properties=properties)


def read_parquet(spark: SparkSession, config: dict, step: str, layer: str, table: str, 
                 date: Optional[str] = None, version: Optional[str] = None) -> DataFrame:
    """
    Read Parquet data from a specified path.
    
    Args:
        spark: SparkSession object.
        config: Configuration dictionary.
        step: Pipeline step (e.g., 'raw', 'cleaned').
        layer: Data layer (e.g., 'transactions', 'reference').
        table: Table name (e.g., 'sales', 'products').
        date (str, optional): Specific date partition (e.g., '2025-09-04').
        version (str, optional): Version for reference data (e.g., '2025-09').
    
    Returns:
        DataFrame: Data read from Parquet.
    """
    path = get_path(config, step, layer, table, version)
    if date:
        path += f"date={date}/"
    return spark.read.parquet(path)


def read_delta(spark: SparkSession, config: dict, step: str, layer: str, table: str, 
               version: Optional[str] = None, timestamp: Optional[str] = None) -> DataFrame:
    """
    Read Delta table data.
    
    Args:
        spark: SparkSession object.
        config: Configuration dictionary.
        step: Pipeline step (e.g., 'curated').
        layer: Data layer (e.g., 'analytics').
        table: Table name (e.g., 'fact_sales').
        version: Specific version to read.
        timestamp: Specific timestamp to read.
    
    Returns:
        DataFrame: Data from Delta table.
    """
    path = get_path(config, step, layer, table)
    
    reader = spark.read.format("delta")
    
    if version:
        reader = reader.option("versionAsOf", version)
    elif timestamp:
        reader = reader.option("timestampAsOf", timestamp)
        
    return reader.load(path)

