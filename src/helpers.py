import zipfile
import pandas as pd
from fnmatch import fnmatch
from pathlib import Path



def read_csv_from_zip(zip_path, pat, usecols=None):
    """
    Read a CSV file from a zip file.
    Climate Trace pattern: pat = "*_emissions_sources_v*.csv"
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        all_files = zip_ref.namelist()
        
        # Pattern to match (works in any subfolder)
        matching_files = [f for f in all_files if fnmatch(f.split('/')[-1], pat)]
        
        if matching_files:
            csv_path = matching_files[0]
            print(f"Reading: {csv_path}")
            
            with zip_ref.open(csv_path) as csv_file:
                power = pd.read_csv(csv_file, usecols=usecols)
                return power
        else:
            print(f"No files found matching pattern: {pat}")