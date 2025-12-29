import os
from dotenv import load_dotenv

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
        "container_name": container_name,
        "tenant_id": os.getenv("AZURE_TENANT_ID"),
        "client_id": os.getenv("AZURE_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_CLIENT_SECRET"),
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

def set_storage_config(spark, config: dict):
    """
    Set Hadoop configuration for ADLS using service principal.
    
    Args:
        spark: SparkSession object.
        config (dict): Configuration dictionary.
    """
    spark.conf.set(
        f"fs.azure.account.auth.type.{config['storage_account_name']}.dfs.core.windows.net",
        "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{config['storage_account_name']}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{config['storage_account_name']}.dfs.core.windows.net",
        config["client_id"]
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{config['storage_account_name']}.dfs.core.windows.net",
        config["client_secret"]
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{config['storage_account_name']}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/token"
    )