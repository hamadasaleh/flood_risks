"""
Logger setup for CaMa-Flood API
"""

from pathlib import Path
from loguru import logger


def setup_logger(log_dir: Path = Path(".logs")) -> None:
    """
    Set up loguru logger with file output.
    
    Args:
        log_dir: Directory to save log files (default: .logs)
    """
    # Create log directory if it doesn't exist
    log_dir.mkdir(exist_ok=True)
    
    # Remove default handler
    logger.remove()
    
    # Add console handler with colors
    logger.add(
        lambda msg: print(msg, end=""),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    # Add file handler
    log_file = log_dir / "cama_flood_api.log"
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="10 MB",
        retention="7 days",
        compression="zip"
    )
    
    logger.info(f"Logger initialized. Log file: {log_file}")

