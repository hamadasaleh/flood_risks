"""
Simple runner for CaMa-Flood model execution
"""

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from src.cama_flood_api.config import CaMaFloodConfig
from src.cama_flood_api.namelist import generate_namelist
from src.cama_flood_api.progress import CaMaFloodProgressTracker


class CaMaFloodRunner:
    """
    Simple runner for executing CaMa-Flood simulations.
    
    Usage:
        config = CaMaFloodConfig(...)
        with CaMaFloodRunner(config) as runner:
            runner.run()
    """
    
    def __init__(self, config: CaMaFloodConfig, show_progress: bool = True):
        """
        Initialize runner with configuration.
        
        Args:
            config: CaMaFlood configuration object
            show_progress: Whether to show progress bar (default: True)
        """
        self.config = config
        self.run_dir = config.get_run_dir()
        self.original_cwd = None
        self.show_progress = show_progress
        self.progress_tracker: Optional[CaMaFloodProgressTracker] = None
    
    def __enter__(self):
        """Context manager entry - prepare for execution"""
        # Save current directory
        self.original_cwd = Path.cwd()
        
        # Create output directory
        self.run_dir.mkdir(parents=True, exist_ok=True)
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup"""
        # Restore original directory
        if self.original_cwd:
            import os
            os.chdir(self.original_cwd)
        return False  # Don't suppress exceptions
    
    def _validate_setup(self) -> None:
        """Validate that all required files exist"""
        # Check executable
        if not self.config.executable_path.exists():
            raise FileNotFoundError(
                f"Executable not found: {self.config.executable_path}"
            )
        
        # Check map directory
        if not self.config.map_dir.exists():
            raise FileNotFoundError(
                f"Map directory not found: {self.config.map_dir}"
            )
        
        # Check dimension info file
        if not self.config.dimension_info_file.exists():
            raise FileNotFoundError(
                f"Dimension info file not found: {self.config.dimension_info_file}"
            )
        
        # Check input matrix file
        if not self.config.input_matrix_file.exists():
            raise FileNotFoundError(
                f"Input matrix file not found: {self.config.input_matrix_file}"
            )
        
        # Check runoff directory
        if not self.config.runoff_dir.exists():
            raise FileNotFoundError(
                f"Runoff directory not found: {self.config.runoff_dir}"
            )
    
    def run(self) -> None:
        """
        Run CaMa-Flood simulation for all years in the configuration.
        
        Runs one year at a time, generating namelist and executing
        the model for each year from start_year to end_year.
        """
        # Validate setup
        self._validate_setup()
        
        # Resolve executable path BEFORE changing directory
        executable_abs_path = self.config.executable_path.resolve()
        logger.info(f"Using executable: {executable_abs_path}")
        
        # Change to run directory
        import os
        os.chdir(self.run_dir)
        
        # Setup progress tracking
        log_file = self.run_dir / "log_CaMa.txt"
        start_date = datetime(self.config.start_year, 1, 1, 0, 0)
        end_date = datetime(self.config.end_year + 1, 1, 1, 0, 0)  # End of end_year
        
        if self.show_progress:
            self.progress_tracker = CaMaFloodProgressTracker(
                log_file=log_file,
                start_date=start_date,
                end_date=end_date,
                enabled=True
            )
            self.progress_tracker.start()
        
        try:
            # Run simulation for each year
            for year in range(self.config.start_year, self.config.end_year + 1):
                logger.info(f"Running year {year}...")
                
                # Generate namelist for this year
                generate_namelist(self.config, self.run_dir, year)
                
                # Run executable
                result = subprocess.run(
                    [str(executable_abs_path)],
                    cwd=self.run_dir,
                    capture_output=True,
                    text=True
                )
                
                # Check for errors
                if result.returncode != 0:
                    logger.error(f"CaMa-Flood failed for year {year}")
                    logger.error(f"Return code: {result.returncode}")
                    if result.returncode == -9:
                        logger.error("Out of memory")
                    logger.error(f"Error output:\n{result.stderr}")
                    raise RuntimeError(
                        f"CaMa-Flood failed for year {year}.\n"
                        f"Return code: {result.returncode}\n"
                        f"Error output:\n{result.stderr}"
                    )
                
                logger.info(f"Year {year} completed successfully")
            
            logger.info(f"Simulation completed. Outputs in: {self.run_dir}")
        finally:
            # Stop progress tracking
            if self.progress_tracker:
                self.progress_tracker.stop()

