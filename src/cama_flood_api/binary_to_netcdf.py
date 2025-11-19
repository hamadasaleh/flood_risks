"""
Convert CaMa-Flood binary output files to NetCDF format.

This module reads Fortran direct-access binary files and converts them to NetCDF.
"""

import numpy as np
from pathlib import Path
from typing import Optional, Tuple
import struct

try:
    import xarray as xr
    XARRAY_AVAILABLE = True
except ImportError:
    XARRAY_AVAILABLE = False

try:
    from netCDF4 import Dataset
    NETCDF4_AVAILABLE = True
except ImportError:
    NETCDF4_AVAILABLE = False

from loguru import logger


def read_diminfo(diminfo_file: Path) -> Tuple[int, int, float, float, float, float]:
    """
    Read grid dimensions and extent from diminfo file.
    
    Format (from CaMa-Flood source):
        Line 1: NX
        Line 2: NY
        Line 3: NLFP
        Line 4: NXIN
        Line 5: NYIN
        Line 6: INPN
        Line 7: filename
        Line 8: WEST
        Line 9: EAST
        Line 10: NORTH
        Line 11: SOUTH
    
    Returns:
        (NX, NY, WEST, EAST, NORTH, SOUTH)
    """
    with open(diminfo_file, 'r') as f:
        lines = [line.strip() for line in f.readlines()]
    
    # Read NX, NY from first two lines (ignore comments after !!)
    NX = int(lines[0].split()[0])
    NY = int(lines[1].split()[0])
    
    # Find WEST, EAST, NORTH, SOUTH (lines 8-11, 0-indexed: 7-10)
    # They may have comments, so split and take first number
    WEST = float(lines[7].split()[0])
    EAST = float(lines[8].split()[0])
    NORTH = float(lines[9].split()[0])
    SOUTH = float(lines[10].split()[0])
    
    return NX, NY, WEST, EAST, NORTH, SOUTH


def read_binary_file(
    binary_file: Path,
    nx: int,
    ny: int,
    dtype: type = np.float32
) -> np.ndarray:
    """
    Read Fortran direct-access binary file.
    
    Args:
        binary_file: Path to .bin file
        nx: Grid dimension in x (longitude)
        ny: Grid dimension in y (latitude)
        dtype: Data type (default: float32, 4 bytes)
    
    Returns:
        3D array with shape (n_timesteps, ny, nx)
    """
    # Record length in bytes (4 bytes per float32, NX*NY grid points)
    record_size = 4 * nx * ny
    
    # Read file to determine number of records
    file_size = binary_file.stat().st_size
    n_records = file_size // record_size
    
    if file_size % record_size != 0:
        logger.warning(
            f"File size {file_size} is not a multiple of record size {record_size}. "
            f"May have incomplete records."
        )
    
    # Read all records
    data = np.zeros((n_records, ny, nx), dtype=dtype)
    
    with open(binary_file, 'rb') as f:
        for rec in range(n_records):
            # Read record (Fortran direct access)
            record_data = f.read(record_size)
            if len(record_data) < record_size:
                logger.warning(f"Record {rec+1} incomplete, stopping")
                break
            
            # Unpack binary data
            values = struct.unpack(f'{nx*ny}f', record_data)
            # Reshape to 2D grid (note: Fortran is column-major, but we read as row-major)
            grid = np.array(values, dtype=dtype).reshape((ny, nx))
            data[rec] = grid
    
    return data


def create_time_coordinates(
    start_year: int,
    n_timesteps: int,
    output_frequency_hours: int = 24
) -> np.ndarray:
    """
    Create time coordinate array.
    
    Args:
        start_year: Start year
        n_timesteps: Number of output timesteps
        output_frequency_hours: Output frequency in hours (default: 24 = daily)
    
    Returns:
        Time array in days since start of year
    """
    # Time in days since start of year
    time_days = np.arange(n_timesteps) * (output_frequency_hours / 24.0)
    return time_days


def create_lon_lat_arrays(
    nx: int,
    ny: int,
    west: float,
    east: float,
    north: float,
    south: float
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Create longitude and latitude coordinate arrays.
    
    Args:
        nx, ny: Grid dimensions
        west, east, north, south: Grid extent
    
    Returns:
        (lon_array, lat_array)
    """
    lon = np.linspace(west, east, nx)
    lat = np.linspace(north, south, ny)
    return lon, lat


def binary_to_netcdf(
    binary_file: Path,
    output_file: Path,
    diminfo_file: Path,
    variable_name: str,
    start_year: int,
    output_frequency_hours: int = 24,
    long_name: Optional[str] = None,
    units: Optional[str] = None
) -> None:
    """
    Convert a single binary output file to NetCDF.
    
    Args:
        binary_file: Path to input .bin file (e.g., rivout1980.bin)
        output_file: Path to output .nc file (e.g., o_rivout1980.nc)
        diminfo_file: Path to diminfo file for grid dimensions
        variable_name: Name of the variable (e.g., 'rivout')
        start_year: Year of the data
        output_frequency_hours: Output frequency in hours (default: 24)
        long_name: Long name for the variable (optional)
        units: Units for the variable (optional)
    """
    if not XARRAY_AVAILABLE and not NETCDF4_AVAILABLE:
        raise ImportError(
            "Either xarray or netCDF4 is required. Install with: pip install xarray netcdf4"
        )
    
    # Read grid dimensions
    nx, ny, west, east, north, south = read_diminfo(diminfo_file)
    logger.info(f"Grid dimensions: NX={nx}, NY={ny}")
    logger.info(f"Grid extent: WEST={west}, EAST={east}, NORTH={north}, SOUTH={south}")
    
    # Read binary data
    logger.info(f"Reading binary file: {binary_file}")
    data = read_binary_file(binary_file, nx, ny)
    n_timesteps = data.shape[0]
    logger.info(f"Read {n_timesteps} timesteps")
    
    # Create coordinates
    time = create_time_coordinates(start_year, n_timesteps, output_frequency_hours)
    lon, lat = create_lon_lat_arrays(nx, ny, west, east, north, south)
    
    # Create NetCDF file
    if XARRAY_AVAILABLE:
        # Use xarray (easier)
        ds = xr.Dataset(
            {
                variable_name: (
                    ['lon', 'lat', 'time'],
                    data.transpose(2, 1, 0)  # Transpose to (nx, ny, time)
                )
            },
            coords={
                'lon': lon,
                'lat': lat,
                'time': time
            }
        )
        
        # Add attributes
        metadata = VARIABLE_METADATA.get(variable_name, {})
        ds[variable_name].attrs['long_name'] = long_name or metadata.get('long_name', variable_name)
        ds[variable_name].attrs['units'] = units or metadata.get('units', '')
        ds['time'].attrs['units'] = f'days since {start_year}-01-01 00:00:00'
        ds['time'].attrs['long_name'] = 'time'
        ds['time'].attrs['calendar'] = 'standard'
        ds['lon'].attrs['units'] = 'degrees_east'
        ds['lon'].attrs['long_name'] = 'longitude'
        ds['lat'].attrs['units'] = 'degrees_north'
        ds['lat'].attrs['long_name'] = 'latitude'
        
        # Write to NetCDF
        logger.info(f"Writing NetCDF file: {output_file}")
        ds.to_netcdf(output_file)
        logger.info(f"Successfully created {output_file}")
    
    else:
        # Use netCDF4 directly
        with Dataset(output_file, 'w', format='NETCDF4') as nc:
            # Create dimensions
            nc.createDimension('time', n_timesteps)
            nc.createDimension('lat', ny)
            nc.createDimension('lon', nx)
            
            # Create variables
            time_var = nc.createVariable('time', 'f8', ('time',))
            lat_var = nc.createVariable('lat', 'f4', ('lat',))
            lon_var = nc.createVariable('lon', 'f4', ('lon',))
            data_var = nc.createVariable(variable_name, 'f4', ('lon', 'lat', 'time'))
            
            # Write coordinates
            time_var[:] = time
            lat_var[:] = lat
            lon_var[:] = lon
            data_var[:] = data.transpose(2, 1, 0)  # (nx, ny, time)
            
            # Add attributes
            time_var.units = f'days since {start_year}-01-01 00:00:00'
            time_var.long_name = 'time'
            time_var.calendar = 'standard'
            lat_var.units = 'degrees_north'
            lat_var.long_name = 'latitude'
            lon_var.units = 'degrees_east'
            lon_var.long_name = 'longitude'
            metadata = VARIABLE_METADATA.get(variable_name, {})
            data_var.long_name = long_name or metadata.get('long_name', variable_name)
            data_var.units = units or metadata.get('units', '')
        
        logger.info(f"Successfully created {output_file}")


def convert_all_binary_outputs(
    output_dir: Path,
    diminfo_file: Path,
    start_year: int,
    end_year: int,
    output_frequency_hours: int = 24,
    variable_names: Optional[list] = None
) -> None:
    """
    Convert all binary output files in a directory to NetCDF.
    
    Args:
        output_dir: Directory containing .bin files
        diminfo_file: Path to diminfo file
        start_year: Start year
        end_year: End year
        output_frequency_hours: Output frequency in hours
        variable_names: List of variable names to convert (if None, auto-detect from .bin files)
    """
    if variable_names is None:
        # Auto-detect variable names from .bin files
        bin_files = list(output_dir.glob("*.bin"))
        # Extract variable names (e.g., rivout1980.bin -> rivout)
        variable_names = []
        seen = set()
        for bin_file in bin_files:
            # Remove year suffix (4 digits) and .bin extension
            name = bin_file.stem
            if len(name) > 4 and name[-4:].isdigit():
                var_name = name[:-4]
                if var_name not in seen:
                    variable_names.append(var_name)
                    seen.add(var_name)
    
    logger.info(f"Converting {len(variable_names)} variables: {variable_names}")
    
    for var_name in variable_names:
        for year in range(start_year, end_year + 1):
            bin_file = output_dir / f"{var_name}{year}.bin"
            nc_file = output_dir / f"o_{var_name}{year}.nc"
            
            if not bin_file.exists():
                logger.warning(f"Binary file not found: {bin_file}, skipping")
                continue
            
            if nc_file.exists():
                logger.info(f"NetCDF file already exists: {nc_file}, skipping")
                continue
            
            try:
                binary_to_netcdf(
                    binary_file=bin_file,
                    output_file=nc_file,
                    diminfo_file=diminfo_file,
                    variable_name=var_name,
                    start_year=year,
                    output_frequency_hours=output_frequency_hours
                )
            except Exception as e:
                logger.error(f"Failed to convert {bin_file}: {e}")
                raise


# Variable metadata (long names and units)
VARIABLE_METADATA = {
    'rivout': {'long_name': 'River discharge', 'units': 'm3/s'},
    'rivsto': {'long_name': 'River storage', 'units': 'm3'},
    'rivdph': {'long_name': 'River water depth', 'units': 'm'},
    'rivvel': {'long_name': 'River velocity', 'units': 'm/s'},
    'fldout': {'long_name': 'Floodplain discharge', 'units': 'm3/s'},
    'fldsto': {'long_name': 'Floodplain storage', 'units': 'm3'},
    'flddph': {'long_name': 'Floodplain water depth', 'units': 'm'},
    'fldfrc': {'long_name': 'Floodplain fraction', 'units': '1'},
    'fldare': {'long_name': 'Floodplain area', 'units': 'm2'},
    'sfcelv': {'long_name': 'Surface water elevation', 'units': 'm'},
    'outflw': {'long_name': 'Outflow', 'units': 'm3/s'},
    'storge': {'long_name': 'Total storage', 'units': 'm3'},
    'pthflw': {'long_name': 'Pathway flow', 'units': 'm3/s'},
    'pthout': {'long_name': 'Pathway output', 'units': 'm3/s'},
    'maxsto': {'long_name': 'Maximum storage', 'units': 'm3'},
    'maxflw': {'long_name': 'Maximum flow', 'units': 'm3/s'},
    'maxdph': {'long_name': 'Maximum depth', 'units': 'm'},
}

