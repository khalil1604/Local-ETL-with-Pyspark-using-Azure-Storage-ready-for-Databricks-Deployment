from src.utils.spark_session import create_spark_session
from src.utils.logging import setup_logging
from src.utils.config import get_config, set_storage_config
from src.ingestion.ingestion_manager import IngestionManager
from src.transformation.transformation_manager import TransformationManager
from src.aggregation.aggregation_manager import AggregationManager
from src.modeling.modeling_manager import ModelingManager

def main():
    logger = setup_logging()
    logger.info("Starting full transformation pipeline")
    
    spark = create_spark_session("TransformationPipeline")
    set_storage_config(spark, get_config())
    
    try:
        ingestion = AggregationManager(spark)
        ingestion.run("monthly")
        logger.info("daily ingestion completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()