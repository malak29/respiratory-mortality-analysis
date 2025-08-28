import sys
from loguru import logger
from app.core.config import settings

def setup_logging():
    """Configure logging for the application"""
    
    # Remove default handler
    logger.remove()
    
    # Console handler with custom format
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    logger.add(
        sys.stdout,
        format=log_format,
        level=settings.LOG_LEVEL,
        colorize=True
    )
    
    # File handler for production
    if settings.ENVIRONMENT == "production":
        logger.add(
            "/var/log/respiratory-api/app.log",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
            level=settings.LOG_LEVEL,
            rotation="100 MB",
            retention="30 days",
            compression="gz"
        )
        
        # Error log file
        logger.add(
            "/var/log/respiratory-api/error.log",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
            level="ERROR",
            rotation="100 MB",
            retention="90 days",
            compression="gz"
        )

def get_logger(name: str):
    """Get logger instance for specific module"""
    return logger.bind(name=name)

# Initialize logging when module is imported
setup_logging()