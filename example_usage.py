"""
Simple example of using the CaMa-Flood Python API

Loads configuration from config.toml file.
"""

from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # tomli for Python < 3.11
    except ImportError:
        raise ImportError(
            "TOML library required. Install with: pip install tomli (Python < 3.11) "
            "or use Python 3.11+ (tomllib included)"
        )

from src.cama_flood_api import CaMaFloodConfig, CaMaFloodRunner

# Load configuration from config.toml
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

print(f"Loaded configuration from {config_path}")
print(f"Experiment: {config.experiment_name}")
print(f"Years: {config.start_year} - {config.end_year}")
print(f"Output format: {'NetCDF' if config.loutcdf else 'Binary'}")

# Run the simulation using context manager
with CaMaFloodRunner(config) as runner:
    runner.run()

print("Done!")

