import logging

def setup_logging(log_file: str = "pipeline.log") -> logging.Logger:
    """
    Configure logging to file and console for all pipeline steps.
    
    Args:
        log_file (str): Path to log file.
    
    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger("data_pipeline")
    logger.setLevel(logging.INFO)
    
    logger.handlers = []
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    
    return logger