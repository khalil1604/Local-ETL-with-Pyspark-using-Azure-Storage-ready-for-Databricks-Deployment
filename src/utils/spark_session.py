from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and configure a Spark session for all pipeline steps.
    
    Args:
        app_name (str): Name of the Spark application.
    
    Returns:
        SparkSession: Configured Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.4,"
                "org.apache.hadoop:hadoop-azure:3.3.6,"
                "org.apache.hadoop:hadoop-azure-datalake:3.3.6,"
                "io.delta:delta-spark_2.13:4.0.0") \
        .getOrCreate()