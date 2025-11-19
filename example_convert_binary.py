"""
Example: Convert CaMa-Flood binary output files to NetCDF format.

This script converts binary output files (.bin) to NetCDF (.nc) format.
Loads configuration from config.toml file.

Usage:
    python example_convert_binary.py [config_path]

Example:
    python example_convert_binary.py
    python example_convert_binary.py ./config.toml
"""

import sys
from pathlib import Path
import tomllib  


from src.cama_flood_api import CaMaFloodConfig
from src.cama_flood_api.binary_to_netcdf import convert_all_binary_outputs

if __name__ == "__main__":
    # Load configuration from config.toml
    if len(sys.argv) > 1:
        config_path = Path(sys.argv[1])
    else:
        config_path = Path(__file__).parent / "config.toml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "rb") as f:
        config_dict = tomllib.load(f)
    
    # Convert string paths to Path objects
    for key in ["base_dir", "map_tar_gz", "runoff_tar", "climatology_tar"]:
        if key in config_dict:
            config_dict[key] = Path(config_dict[key])
    
    # Handle optional paths
    if "executable_path" in config_dict and config_dict["executable_path"]:
        config_dict["executable_path"] = Path(config_dict["executable_path"])
    
    if "dimension_info_file" in config_dict and config_dict["dimension_info_file"]:
        config_dict["dimension_info_file"] = Path(config_dict["dimension_info_file"])
    
    if "input_matrix_file" in config_dict and config_dict["input_matrix_file"]:
        config_dict["input_matrix_file"] = Path(config_dict["input_matrix_file"])
    
    # Create config from TOML
    config = CaMaFloodConfig(**config_dict)
    
    # Get output directory and diminfo file from config
    output_dir = config.get_output_dir()
    diminfo_file = config.dimension_info_file
    
    # Validate paths
    if not output_dir.exists():
        print(f"Error: Output directory not found: {output_dir}")
        print("Make sure the simulation has been run first.")
        sys.exit(1)
    
    if not diminfo_file or not diminfo_file.exists():
        print(f"Error: Diminfo file not found: {diminfo_file}")
        sys.exit(1)
    
    print(f"Loaded configuration from {config_path}")
    print(f"Output directory: {output_dir}")
    print(f"Diminfo file: {diminfo_file}")
    print(f"Years: {config.start_year} to {config.end_year}")
    print(f"Output frequency: {config.ifrq_out} hours")
    print()
    
    # Convert all binary files to NetCDF
    convert_all_binary_outputs(
        output_dir=output_dir,
        diminfo_file=diminfo_file,
        start_year=config.start_year,
        end_year=config.end_year,
        output_frequency_hours=config.ifrq_out
    )
    
    print("\nConversion complete!")

