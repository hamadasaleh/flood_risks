"""
Simple example of using the CaMa-Flood Python API
"""

from src.cama_flood_api import CaMaFloodConfig, CaMaFloodRunner

# Option 1: Using tar.gz and tar files (will be extracted automatically)
# map_name is auto-extracted from filename (glb_01min.tar.gz -> glb_01min)
# runoff_name is auto-extracted from filename (E2O_ecmwf.tar -> E2O_ecmwf)
# runoff_prefix is auto-detected from files in the extracted directory
config = CaMaFloodConfig(
    base_dir="./cama_flood",
    map_tar_gz="./data/glb_15min.tar.gz",  # Extracts to cama_flood/map/glb_15min/
    runoff_tar="./data/E2O_ecmwf.tar",  # Extracts to cama_flood/inp/E2O_ecmwf/
    climatology_tar="./data/climatology_runoff.tar.gz",  # Extracts to cama_flood/map/data/
    start_year=1980,
    end_year=1981,  # Just 2 years for testing
    # executable_path will default to ./cama_flood/src/MAIN_cmf if not provided
    # auto_compile=True will automatically compile if executable doesn't exist
    experiment_name="my_test_simulation"
    # Time resolution parameters use defaults:
    # dt=3600 (1 hour), ladpstp=True, pcadp=0.7, ifrq_inp=24, ifrq_out=24
)



# Run the simulation using context manager
with CaMaFloodRunner(config) as runner:
    runner.run()

print("Done!")

