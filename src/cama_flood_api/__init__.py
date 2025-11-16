"""
CaMa-Flood Python API
"""

# Initialize logger on import
from src.cama_flood_api.logger import setup_logger
setup_logger()

from src.cama_flood_api.config import CaMaFloodConfig
from src.cama_flood_api.runner import CaMaFloodRunner

__version__ = "0.1.0"
__all__ = ["CaMaFloodConfig", "CaMaFloodRunner"]

