import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def get_config() -> dict:
    """
    Load environment variables and return configuration dictionary.
    
    Returns:
        dict: Configuration for storage and DB.
    """
    load_dotenv()
    storage_account_name = "firstprojectde"
    container_name = "steps"
    return {
        "storage_account_name": storage_account_name,
        "storage_account_key": os.getenv("AZURE_STORAGE_KEY"),
        "container_name": container_name,
        "jdbc_url": "jdbc:postgresql://localhost:5433/retail_db",
        "db_properties": {
            "user": "admin",
            "password": "admin123",
            "driver": "org.postgresql.Driver"
        },
        "base_path": f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    }

def get_path(config: dict, step: str, layer: str, table: str, version: str = None) -> str:
    """
    Generate path for any layer data.
    
    Args:
        config (dict): Configuration dictionary.
        step (str): Pipeline step (e.g., 'raw', 'cleaned', 'curated').
        layer (str): Data layer (e.g., 'transactions', 'reference').
        table (str): Table name (e.g., 'sales', 'products').
        version (str, optional): Version for reference data (e.g., '2025-07').
    
    Returns:
        str: Path for writing data.
    """
    base_path = config["base_path"]
    if version:
        return f"{base_path}{step}/{layer}/{table}/version={version}/"
    return f"{base_path}{step}/{layer}/{table}/"

def set_storage_config(spark : SparkSession, config: dict):
    """
    Set Hadoop configuration for ADLS.
    
    Args:
        spark: SparkSession object.
        config (dict): Configuration dictionary.
    """
    if "abfss" in config["base_path"]:
        spark.conf.set(
            f"fs.azure.account.key.{config['storage_account_name']}.dfs.core.windows.net",
            config["storage_account_key"]
        )