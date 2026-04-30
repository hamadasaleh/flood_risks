"""
Configuration for CaMa-Flood simulations
"""

import os
import re
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from loguru import logger


class CaMaFloodConfig(BaseModel):
    """
    Minimal configuration for CaMa-Flood simulation.
    
    Only the 8 essential parameters are required. All other settings
    use sensible defaults for E2O runoff with global 15min map.
    """
    
    # Required parameters
    base_dir: Path = Field(..., description="Base directory of CaMa-Flood installation")
    start_year: int = Field(..., description="Simulation start year")
    end_year: int = Field(..., description="Simulation end year")
    executable_path: Optional[Path] = Field(
        None,
        description="Path to compiled CaMa-Flood executable. If not provided, will be set to base_dir/src/MAIN_cmf"
    )
    experiment_name: str = Field(..., description="Experiment name (used for output directory)")
    
    # Runoff configuration - required tar file, will be extracted
    runoff_tar: Path = Field(
        ...,
        description="Path to runoff tar file (e.g., 'E2O_ecmwf.tar'). Will be extracted to base_dir/inp/{name_from_file}/"
    )
    runoff_dir: Optional[Path] = Field(
        None,
        description="Path to extracted runoff directory (set automatically after extraction)"
    )
    runoff_prefix: Optional[str] = Field(
        None,
        description="Prefix for runoff NetCDF files (e.g., 'e2o_ecmwf_wrr2_glob15_day_Runoff_'). Auto-detected if not provided"
    )
    
    # Map configuration - required tar.gz file, will be extracted
    map_tar_gz: Path = Field(
        ...,
        description="Path to map tar.gz file (e.g., 'glb_15min.tar.gz'). Will be extracted to base_dir/map/{name_from_file}/"
    )
    map_dir: Optional[Path] = Field(
        None, 
        description="Path to extracted map directory (set automatically after extraction)"
    )
    map_name: Optional[str] = Field(
        None,
        description="Name for extracted map directory (e.g., 'glb_15min'). Auto-extracted from map_tar_gz filename if not provided"
    )
    
    # Climatology configuration - required tar/tar.gz file, will be extracted to map/data/
    climatology_tar: Path = Field(
        ...,
        description="Path to climatology tar/tar.gz file (e.g., 'climatology_runoff.tar.gz'). Will be extracted to base_dir/map/data/"
    )
    
    # Optional: can be inferred from map_dir if using standard naming
    dimension_info_file: Optional[Path] = Field(
        None, 
        description="Path to diminfo file (defaults to map_dir/diminfo_test-15min_nc.txt)"
    )
    input_matrix_file: Optional[Path] = Field(
        None,
        description="Path to input matrix file (defaults to map_dir/inpmat_test-15min_nc.bin)"
    )
    
    # Time resolution parameters
    dt: int = Field(
        3600,
        description="Main timestep length in seconds (default: 3600 = 1 hour). Must be multiple of 60."
    )
    ladpstp: bool = Field(
        True,
        description="Use adaptive time stepping (default: True). If False, uses fixed DT timestep."
    )
    pcadp: float = Field(
        0.7,
        description="Courant coefficient for adaptive timestep calculation (default: 0.7). Lower = smaller timesteps (more stable, slower)."
    )
    ifrq_inp: int = Field(
        24,
        description="Input forcing update frequency in hours (default: 24 = daily). How often to read new input data."
    )
    ifrq_out: int = Field(
        24,
        description="Output data write frequency in hours (default: 24 = daily). How often to write averaged output files."
    )
    shour: int = Field(
        0,
        description="Simulation start hour (0-23). Use 12 to align model clock with noon-based forcing."
    )
    shourin: int = Field(
        0,
        description="Start hour of NetCDF input forcing time axis (0-23). Use 12 if forcing timestamps are at 12:00."
    )
    drofunit: int = Field(
        86400000,
        description="Runoff unit conversion factor used by CaMa-Flood (InpUnit / DROFUNIT = m/s)."
    )
    loutcdf: bool = Field(
        False,
        description="Use NetCDF format for output files (default: False = binary format). If True, outputs are .nc files instead of .bin files."
    )
    output_variables: str = Field(
        "rivout,rivsto,rivdph,rivvel,fldout,fldsto,flddph,fldfrc,fldare,sfcelv,outflw,storge,pthflw,pthout,maxsto,maxflw,maxdph",
        description="Comma-separated CaMa-Flood output variables written to CVARSOUT."
    )
    
    @field_validator('base_dir', mode='before')
    @classmethod
    def convert_base_dir_to_path(cls, v):
        """Convert base_dir string to Path object"""
        if v is None:
            return None
        if isinstance(v, str):
            return Path(v)
        return v
    
    @field_validator('base_dir', mode='after')
    @classmethod
    def resolve_base_dir(cls, v):
        """Resolve base_dir to absolute path"""
        if v is not None:
            return v.resolve()
        return v
    
    @field_validator('runoff_dir', 'map_dir', 'map_tar_gz', 'runoff_tar', 'climatology_tar', mode='before')
    @classmethod
    def convert_to_path(cls, v):
        """Convert string paths to Path objects"""
        if v is None:
            return None
        if isinstance(v, str):
            return Path(v)
        return v
    
    @field_validator('executable_path', mode='before')
    @classmethod
    def convert_executable_path(cls, v):
        """Convert string executable path to Path object"""
        if v is None:
            return None
        if isinstance(v, str):
            return Path(v)
        return v
    
    @field_validator('dimension_info_file', 'input_matrix_file', mode='before')
    @classmethod
    def convert_optional_path(cls, v):
        """Convert optional string paths to Path objects"""
        if v is None:
            return None
        if isinstance(v, str):
            return Path(v)
        return v
    
    @model_validator(mode='after')
    def validate_map_config(self):
        """Validate map configuration and extract tar.gz"""
        # Extract map from tar.gz (map_tar_gz is now required)
        if not self.map_tar_gz.exists():
            raise FileNotFoundError(f"Map tar.gz file not found: {self.map_tar_gz}")
        
        # Extract map_name from filename if not provided
        if self.map_name is None:
            # Remove .tar.gz or .tgz extension to get map name
            filename = self.map_tar_gz.stem  # Gets filename without extension
            if filename.endswith('.tar'):
                filename = filename[:-4]  # Remove .tar if present
            self.map_name = filename
        
        # Target extraction directory: base_dir/map/{map_name}/
        map_extract_dir = self.base_dir / "map" / self.map_name
        map_extract_dir.parent.mkdir(parents=True, exist_ok=True)
        
        # Only extract if directory doesn't exist or is empty
        if not map_extract_dir.exists() or not any(map_extract_dir.iterdir()):
            logger.info(f"Extracting map from {self.map_tar_gz} to {map_extract_dir}...")
            
            # Extract to temporary location first
            temp_extract = map_extract_dir.parent / f".temp_{self.map_name}"
            temp_extract.mkdir(exist_ok=True)
            
            try:
                with tarfile.open(self.map_tar_gz, 'r:gz') as tar:
                    tar.extractall(temp_extract)
                
                # Find what was extracted (handle different tar structures)
                extracted_items = list(temp_extract.iterdir())
                
                if len(extracted_items) == 1 and extracted_items[0].is_dir():
                    # Single directory extracted - use it as the map directory
                    extracted_dir = extracted_items[0]
                    extracted_dir.rename(map_extract_dir)
                else:
                    # Multiple items or files at root - create map_name directory and move contents
                    map_extract_dir.mkdir(exist_ok=True)
                    for item in extracted_items:
                        item.rename(map_extract_dir / item.name)
                
                logger.info(f"Map extracted successfully to {map_extract_dir}")
            finally:
                # Clean up temp directory
                if temp_extract.exists():
                    shutil.rmtree(temp_extract)
        else:
            logger.info(f"Map directory already exists at {map_extract_dir}, skipping extraction")
        
        # Set map_dir to extracted location (already a Path object)
        self.map_dir = map_extract_dir
        
        # Set default paths for dimension_info_file and input_matrix_file
        if self.dimension_info_file is None:
            self.dimension_info_file = self.map_dir / "diminfo_test-1deg_grid.txt"
        if self.input_matrix_file is None:
            self.input_matrix_file = self.map_dir / "inpmat_test-1deg_grid.bin"
        
        # Validate and extract runoff if needed
        self._validate_and_extract_runoff()
        
        # Extract climatology if provided
        self._validate_and_extract_climatology()
        
        # Set default executable path if not provided
        if self.executable_path is None:
            self.executable_path = self.base_dir / "src" / "MAIN_cmf"
        
        # Always compile executable
        logger.info(f"Compiling executable at {self.executable_path}...")
        self._compile_executable()
        
        # Generate channel parameters if needed (rivhgt.bin, rivwth.bin, etc.)
        self._generate_channel_params()
        
        # Log configuration
        logger.info("CaMa-Flood Configuration:")
        logger.info(f"  Base directory: {self.base_dir}")
        logger.info(f"  Map tar.gz: {self.map_tar_gz}")
        logger.info(f"  Map directory: {self.map_dir}")
        logger.info(f"  Climatology tar: {self.climatology_tar}")
        logger.info(f"  Runoff tar: {self.runoff_tar}")
        logger.info(f"  Runoff directory: {self.runoff_dir}")
        logger.info(f"  Runoff prefix: {self.runoff_prefix}")
        logger.info(f"  Start year: {self.start_year}")
        logger.info(f"  End year: {self.end_year}")
        logger.info(f"  Executable: {self.executable_path}")
        logger.info(f"  Experiment: {self.experiment_name}")
        logger.info(f"  Time resolution:")
        logger.info(f"    DT (main timestep): {self.dt} seconds ({self.dt/3600:.1f} hours)")
        logger.info(f"    Adaptive timestep: {self.ladpstp}")
        if self.ladpstp:
            logger.info(f"    Courant coefficient (PCADP): {self.pcadp}")
        logger.info(f"    Input frequency: {self.ifrq_inp} hours")
        logger.info(f"    Output frequency: {self.ifrq_out} hours")
        logger.info(f"    Runoff conversion (DROFUNIT): {self.drofunit}")
        logger.info(f"    Output format: {'NetCDF' if self.loutcdf else 'Binary'}")
        logger.info(f"    Output variables: {self.output_variables}")
        logger.debug(f"Dimension info file: {self.dimension_info_file}")
        logger.debug(f"Input matrix file: {self.input_matrix_file}")
        
        return self
    
    def _validate_and_extract_runoff(self):
        """Validate runoff configuration and extract tar"""
        # Extract runoff from tar (runoff_tar is now required)
        if not self.runoff_tar.exists():
            raise FileNotFoundError(f"Runoff tar file not found: {self.runoff_tar}")
        
        # Extract runoff_name from filename if not provided
        # Remove .tar extension to get runoff name
        filename = self.runoff_tar.stem  # Gets filename without extension
        runoff_name = filename
        
        # Target extraction directory: base_dir/inp/{runoff_name}/
        runoff_extract_dir = self.base_dir / "inp" / runoff_name
        runoff_extract_dir.parent.mkdir(parents=True, exist_ok=True)
        
        # Only extract if directory doesn't exist or is empty
        if not runoff_extract_dir.exists() or not any(runoff_extract_dir.iterdir()):
            logger.info(f"Extracting runoff from {self.runoff_tar} to {runoff_extract_dir}...")
            
            # Extract to temporary location first
            temp_extract = runoff_extract_dir.parent / f".temp_{runoff_name}"
            temp_extract.mkdir(exist_ok=True)
            
            try:
                with tarfile.open(self.runoff_tar, 'r:') as tar:  # 'r:' for uncompressed tar
                    tar.extractall(temp_extract)
                
                # Find what was extracted (handle different tar structures)
                extracted_items = list(temp_extract.iterdir())
                
                if len(extracted_items) == 1 and extracted_items[0].is_dir():
                    # Single directory extracted - use it as the runoff directory
                    extracted_dir = extracted_items[0]
                    extracted_dir.rename(runoff_extract_dir)
                else:
                    # Multiple items or files at root - create runoff_name directory and move contents
                    runoff_extract_dir.mkdir(exist_ok=True)
                    for item in extracted_items:
                        item.rename(runoff_extract_dir / item.name)
                
                logger.info(f"Runoff extracted successfully to {runoff_extract_dir}")
            finally:
                # Clean up temp directory
                if temp_extract.exists():
                    shutil.rmtree(temp_extract)
        else:
            logger.info(f"Runoff directory already exists at {runoff_extract_dir}, skipping extraction")
        
        # Set runoff_dir to extracted location
        self.runoff_dir = runoff_extract_dir
        
        # Auto-detect runoff_prefix if not provided
        if self.runoff_prefix is None:
            self.runoff_prefix = self._detect_runoff_prefix()
    
    def _validate_and_extract_climatology(self):
        """Validate climatology configuration and extract tar/tar.gz to map/data/"""
        if not self.climatology_tar.exists():
            raise FileNotFoundError(f"Climatology tar file not found: {self.climatology_tar}")
        
        # Target extraction directory: base_dir/map/data/
        # Ensure base_dir is resolved to absolute path to avoid nested paths
        climatology_extract_dir = (self.base_dir.resolve() / "map" / "data").resolve()
        climatology_extract_dir.mkdir(parents=True, exist_ok=True)
        
        # Only extract if directory doesn't exist or is empty
        if not climatology_extract_dir.exists() or not any(climatology_extract_dir.iterdir()):
            logger.info(f"Extracting climatology from {self.climatology_tar} to {climatology_extract_dir}...")
            
            # Determine if it's a tar.gz or regular tar
            is_gzipped = str(self.climatology_tar).endswith('.gz')
            
            # Extract to temporary location first
            temp_extract = climatology_extract_dir.parent / ".temp_data"
            temp_extract.mkdir(exist_ok=True)
            
            try:
                if is_gzipped:
                    with tarfile.open(self.climatology_tar, 'r:gz') as tar:
                        tar.extractall(temp_extract)
                else:
                    with tarfile.open(self.climatology_tar, 'r:') as tar:
                        tar.extractall(temp_extract)
                
                # Find and extract only the .one file to target directory
                # Search recursively for .one files
                one_files = list(temp_extract.rglob("*.one"))
                
                if not one_files:
                    raise FileNotFoundError(
                        f"No .one file found in climatology tar: {self.climatology_tar}"
                    )
                
                if len(one_files) > 1:
                    logger.warning(
                        f"Multiple .one files found in tar, using first one: {one_files[0].name}"
                    )
                
                # Extract the .one file to target directory
                source_file = one_files[0]
                target_file = climatology_extract_dir.resolve() / source_file.name
                
                if target_file.exists():
                    target_file.unlink()
                
                source_file.rename(target_file)
                logger.debug(f"Extracted {source_file.name} to {target_file}")
                
                logger.info(f"Climatology extracted successfully to {climatology_extract_dir}")
            finally:
                # Clean up temp directory
                if temp_extract.exists():
                    shutil.rmtree(temp_extract)
        else:
            logger.info(f"Climatology directory already exists at {climatology_extract_dir}, skipping extraction")
    
    def _detect_runoff_prefix(self) -> str:
        """
        Auto-detect runoff prefix by scanning the runoff directory for NetCDF files.
        
        Looks for files matching pattern: {prefix}YYYY.nc and extracts the prefix.
        """
        if self.runoff_dir is None or not self.runoff_dir.exists():
            raise ValueError(
                "Cannot auto-detect runoff_prefix: runoff_dir is not set or doesn't exist. "
                "Please provide runoff_prefix explicitly."
            )
        
        # Find all .nc files in the directory
        nc_files = list(self.runoff_dir.glob("*.nc"))
        
        if not nc_files:
            raise ValueError(
                f"No NetCDF files found in {self.runoff_dir}. "
                "Cannot auto-detect runoff_prefix. Please provide it explicitly."
            )
        
        # Try to find pattern: {prefix}YYYY.nc where YYYY is a 4-digit year
        pattern = re.compile(r'^(.+?)(\d{4})\.nc$')
        
        prefixes = set()
        for nc_file in nc_files:
            match = pattern.match(nc_file.name)
            if match:
                prefix = match.group(1)
                year = match.group(2)
                # Validate year is reasonable (1900-2100)
                if 1900 <= int(year) <= 2100:
                    prefixes.add(prefix)
        
        if not prefixes:
            raise ValueError(
                f"Could not detect runoff prefix pattern in {self.runoff_dir}. "
                "Files should match pattern: {prefix}YYYY.nc. Please provide runoff_prefix explicitly."
            )
        
        if len(prefixes) > 1:
            raise ValueError(
                f"Multiple prefix patterns found in {self.runoff_dir}: {prefixes}. "
                "Please provide runoff_prefix explicitly to specify which one to use."
            )
        
        detected_prefix = list(prefixes)[0]
        logger.info(f"Auto-detected runoff_prefix: '{detected_prefix}'")
        return detected_prefix
    
    
    @field_validator('start_year', 'end_year')
    @classmethod
    def validate_years(cls, v):
        """Validate year is reasonable"""
        if not (1900 <= v <= 2100):
            raise ValueError(f"Year {v} is outside reasonable range (1900-2100)")
        return v
    
    @field_validator('end_year')
    @classmethod
    def validate_end_year(cls, v, info):
        """Validate end_year is after start_year"""
        if 'start_year' in info.data and v < info.data['start_year']:
            raise ValueError(f"end_year ({v}) must be >= start_year ({info.data['start_year']})")
        return v
    
    @field_validator('dt')
    @classmethod
    def validate_dt(cls, v):
        """Validate DT is multiple of 60 seconds"""
        if v < 60 or v % 60 != 0:
            raise ValueError(f"dt ({v}) must be >= 60 and a multiple of 60 seconds")
        return v
    
    @field_validator('ifrq_inp', 'ifrq_out')
    @classmethod
    def validate_frequency(cls, v):
        """Validate frequency is positive"""
        if v <= 0:
            raise ValueError(f"Frequency must be > 0, got {v}")
        return v

    @field_validator('shour', 'shourin')
    @classmethod
    def validate_hour_field(cls, v):
        """Validate hour field is in valid range"""
        if not (0 <= v <= 23):
            raise ValueError(f"Hour must be between 0 and 23, got {v}")
        return v

    @field_validator('drofunit')
    @classmethod
    def validate_drofunit(cls, v):
        """Validate runoff conversion factor is positive"""
        if v <= 0:
            raise ValueError(f"drofunit must be > 0, got {v}")
        return v
    
    @field_validator('pcadp')
    @classmethod
    def validate_pcadp(cls, v):
        """Validate PCADP is in reasonable range"""
        if not (0.1 <= v <= 1.0):
            raise ValueError(f"pcadp ({v}) should be between 0.1 and 1.0 for stability")
        return v

    @field_validator('output_variables')
    @classmethod
    def validate_output_variables(cls, v):
        """Validate output variable list is not empty"""
        if not v or not v.strip():
            raise ValueError("output_variables must be a non-empty comma-separated string")
        return v.strip()
    
    def get_output_dir(self) -> Path:
        """Get the output directory path"""
        # base_dir is already resolved to absolute path in validator
        return self.base_dir / "out" / self.experiment_name
    
    def get_run_dir(self) -> Path:
        """Get the run directory path (same as output for now)"""
        return self.get_output_dir()
    
    def _compile_executable(self) -> None:
        """
        Compile the CaMa-Flood executable.
        
        Compiles libraries in util, map/src/src_param, map/src/src_region, src,
        then compiles MAIN_cmf in src directory.
        """
        logger.info("Starting CaMa-Flood compilation...")
        
        base_path = self.base_dir.resolve()
        
        # Libraries to compile
        libs = ["util", "map/src/src_param", "map/src/src_region", "src"]
        
        # Compile each library
        for lib in libs:
            lib_path = base_path / lib
            if not lib_path.exists():
                raise FileNotFoundError(f"Library directory not found: {lib_path}")
            
            logger.info(f"Compiling library: {lib}")
            result = subprocess.run(
                ["make", "all"],
                cwd=lib_path,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                logger.error(f"Compilation failed for {lib}")
                logger.error(f"Error output:\n{result.stderr}")
                raise RuntimeError(f"Failed to compile library {lib}:\n{result.stderr}")
        
        # Compile MAIN_cmf executable
        src_path = base_path / "src"
        logger.info("Compiling MAIN_cmf executable...")
        result = subprocess.run(
            ["make", "MAIN_cmf"],
            cwd=src_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error("Failed to compile MAIN_cmf")
            logger.error(f"Error output:\n{result.stderr}")
            raise RuntimeError(f"Failed to compile MAIN_cmf:\n{result.stderr}")
        
        # Verify executable was created
        if not self.executable_path.exists():
            raise RuntimeError(
                f"Compilation completed but executable not found at {self.executable_path}"
            )
        
        logger.info(f"Compilation successful! Executable created at {self.executable_path}")
    
    def _generate_channel_params(self) -> None:
        """
        Generate channel parameters (rivhgt.bin, rivwth.bin, etc.) using s01-channel_params.sh.
        
        This script must be run from the map directory and requires:
        - Map directory with diminfo file
        - Climatology data in map/data/
        - Compiled executables in map/src/src_param/
        """
        # Check if required output files already exist
        rivhgt_file = self.map_dir / "rivhgt.bin"
        rivwth_file = self.map_dir / "rivwth.bin"
        rivwth_gwdlr_file = self.map_dir / "rivwth_gwdlr.bin"
        bifprm_file = self.map_dir / "bifprm.txt"
        
        # If all key files exist, skip generation
        if all(f.exists() and f.stat().st_size > 0 for f in [rivhgt_file, rivwth_file, rivwth_gwdlr_file]):
            logger.info(f"Channel parameters already exist in {self.map_dir}, skipping generation")
            return
        
        logger.info("Generating channel parameters (rivhgt.bin, rivwth.bin, etc.)...")
        
        # Check prerequisites
        src_param_dir = self.base_dir / "map" / "src" / "src_param"
        if not src_param_dir.exists():
            raise FileNotFoundError(
                f"Source parameter directory not found: {src_param_dir}. "
                "This is required to generate channel parameters."
            )
        
        # Check if executables exist
        calc_outclm = src_param_dir / "calc_outclm"
        calc_rivwth = src_param_dir / "calc_rivwth"
        if not calc_outclm.exists() or not calc_rivwth.exists():
            logger.warning(
                f"Channel parameter executables not found in {src_param_dir}. "
                "Attempting to compile..."
            )
            # Compile the parameter generation tools
            result = subprocess.run(
                ["make", "all"],
                cwd=src_param_dir,
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to compile channel parameter tools:\n{result.stderr}"
                )
        
        # Check climatology file exists
        climatology_file = self.base_dir / "map" / "data" / "ELSE_GPCC_clm-1981-2010.one"
        if not climatology_file.exists():
            # Try to find any .one file in data directory
            data_dir = self.base_dir / "map" / "data"
            one_files = list(data_dir.glob("*.one"))
            if one_files:
                climatology_file = one_files[0]
                logger.info(f"Using climatology file: {climatology_file.name}")
            else:
                raise FileNotFoundError(
                    f"Climatology file not found. Expected: {climatology_file} "
                    f"or any .one file in {data_dir}"
                )
        
        # Check if s01-channel_params.sh exists
        script_path = src_param_dir / "s01-channel_params.sh"
        
        # Save current directory
        import os
        original_cwd = Path.cwd()
        
        try:
            # Change to map directory (where the script expects to run from)
            os.chdir(self.map_dir)
            
            if script_path.exists():
                # The script expects to be run from map/{map_name}/ and does 'cd ..' to go to map/
                # It also expects src_param to be at map/src_param/, but it's actually at map/src/src_param/
                # So we need to create a symlink or copy, or adjust paths
                # For now, let's run the commands directly with correct paths
                logger.info("Running channel parameter generation commands...")
                
                # Step 1: calc_outclm
                logger.info("Running calc_outclm...")
                # Climatology file is at map/data/, map_dir is at map/glb_15min/
                # So relative path from map_dir is ../data/{filename}
                climatology_relative = Path("..") / "data" / climatology_file.name
                result = subprocess.run(
                    [
                        str(calc_outclm),
                        "bin",
                        "inpmat",
                        str(self.dimension_info_file.name),  # Use relative path from map_dir
                        str(climatology_relative)
                    ],
                    cwd=self.map_dir,
                    capture_output=True,
                    text=True
                )
                if result.returncode != 0:
                    logger.error(f"calc_outclm failed:")
                    logger.error(f"stdout:\n{result.stdout}")
                    logger.error(f"stderr:\n{result.stderr}")
                    raise RuntimeError(f"calc_outclm failed:\n{result.stderr}")
            
            # Step 2: calc_rivwth (with default parameters from script)
            logger.info("Running calc_rivwth...")
            result = subprocess.run(
                [
                    str(calc_rivwth),
                    "bin",
                    str(self.dimension_info_file.name),
                    "0.1",      # HC
                    "0.50",     # HP
                    "0.00",     # HO
                    "1.0",      # HMIN
                    "2.50",     # WC
                    "0.60",     # WP
                    "0.00",     # WO
                    "5.0"       # WMIN
                ],
                cwd=self.map_dir,
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                logger.error(f"calc_rivwth failed:")
                logger.error(f"stdout:\n{result.stdout}")
                logger.error(f"stderr:\n{result.stderr}")
                raise RuntimeError(f"calc_rivwth failed:\n{result.stderr}")
            
            # Step 3: set_gwdlr
            set_gwdlr = src_param_dir / "set_gwdlr"
            if set_gwdlr.exists():
                logger.info("Running set_gwdlr...")
                result = subprocess.run(
                    [str(set_gwdlr), str(self.dimension_info_file.name)],
                    cwd=self.map_dir,
                    capture_output=True,
                    text=True
                )
                if result.returncode != 0:
                    logger.warning(f"set_gwdlr failed (may be optional):\n{result.stderr}")
            
            # Step 4: set_bifparam
            set_bifparam = src_param_dir / "set_bifparam"
            if set_bifparam.exists():
                logger.info("Running set_bifparam...")
                result = subprocess.run(
                    [
                        str(set_bifparam),
                        "rivhgt.bin",
                        "bifprm.txt",
                        "5",  # BIFLAYER
                        str(self.dimension_info_file.name)
                    ],
                    cwd=self.map_dir,
                    capture_output=True,
                    text=True
                )
                if result.returncode != 0:
                    logger.warning(f"set_bifparam failed (may be optional):\n{result.stderr}")
            
            # Verify key files were created
            if not rivhgt_file.exists() or rivhgt_file.stat().st_size == 0:
                raise RuntimeError(f"rivhgt.bin was not created or is empty in {self.map_dir}")
            if not rivwth_file.exists() or rivwth_file.stat().st_size == 0:
                raise RuntimeError(f"rivwth.bin was not created or is empty in {self.map_dir}")
            
            logger.info("Channel parameters generated successfully")
            
        finally:
            # Restore original directory
            os.chdir(original_cwd)

