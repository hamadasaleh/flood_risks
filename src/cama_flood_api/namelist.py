"""
Namelist generator for CaMa-Flood
"""
import f90nml
from pathlib import Path
from src.cama_flood_api.config import CaMaFloodConfig


def generate_namelist(config: CaMaFloodConfig, output_path: Path, year: int) -> None:
    """
    Generate input_cmf.nam namelist file for a single year simulation.
    
    Args:
        config: CaMaFlood configuration
        output_path: Path where namelist file will be written (run directory)
        year: Simulation year
    """
    namelist_path = output_path / "input_cmf.nam"
    
    # Calculate dates
    syear = year
    smon = 1
    sday = 1
    shour = config.shour
    eyear = year + 1
    emon = 1
    eday = 1
    ehour = 0
    
    # NetCDF runoff file for this year
    runoff_file = config.runoff_dir / f"{config.runoff_prefix}{year:04d}.nc"
    
    # Output tag
    couttag = f"{year:04d}"
    
    # Check if bifurcation file exists and is not empty
    bifprm_file = config.map_dir / "bifprm.txt"
    lpthout = False  # Default to False
    if bifprm_file.exists():
        # Check if file has content (at least first line with two integers)
        try:
            with open(bifprm_file, 'r') as f:
                first_line = f.readline().strip()
                if first_line and len(first_line.split()) >= 2:
                    # Try to parse as integers to validate format
                    parts = first_line.split()
                    int(parts[0])
                    int(parts[1])
                    lpthout = True
        except (ValueError, IndexError):
            # File exists but doesn't have valid format
            lpthout = False
    
    # Generate namelist content as dictionary
    # Note: Use Python booleans (True/False) - f90nml will convert to .TRUE./.FALSE.
    namelist_dict = {
        "NRUNVER": {
            "LADPSTP": config.ladpstp,  # true: use adaptive time step
            "LPTHOUT": lpthout,  # true: activate bifurcation scheme (only if bifprm.txt is valid)
            "LRESTART": False,  # true: initial condition from restart file
        },
        "NDIMTIME": {
            "CDIMINFO": str(config.dimension_info_file),  # text file for dimention information
            "DT": config.dt,  # time step length (sec)
            "IFRQ_INP": config.ifrq_inp,  # input forcing update frequency (hour)
        },
        "NPARAM": {
            "PMANRIV": 0.03,  # manning coefficient river
            "PMANFLD": 0.10,  # manning coefficient floodplain
            "PDSTMTH": 10000.0,  # downstream distance at river mouth [m]
            "PCADP": config.pcadp,  # CFL coefficient for adaptive timestep
        },
        "NSIMTIME": {
            "SYEAR": syear,  # start year
            "SMON": smon,  # month
            "SDAY": sday,  # day
            "SHOUR": shour,  # hour
            "EYEAR": eyear,  # end year
            "EMON": emon,  # month
            "EDAY": eday,  # day
            "EHOUR": ehour,  # hour
        },
        "NMAP": {
            "LMAPCDF": False,  # * true for netCDF map input
            "CNEXTXY": str(config.map_dir / "nextxy.bin"),  # river network nextxy
            "CGRAREA": str(config.map_dir / "ctmare.bin"),  # catchment area
            "CELEVTN": str(config.map_dir / "elevtn.bin"),  # bank top elevation
            "CNXTDST": str(config.map_dir / "nxtdst.bin"),  # distance to next outlet
            "CRIVLEN": str(config.map_dir / "rivlen.bin"),  # river channel length
            "CFLDHGT": str(config.map_dir / "fldhgt.bin"),  # floodplain elevation profile
            "CRIVWTH": str(config.map_dir / "rivwth_gwdlr.bin"),  # channel width
            "CRIVHGT": str(config.map_dir / "rivhgt.bin"),  # channel depth
            "CRIVMAN": str(config.map_dir / "rivman.bin"),  # river manning coefficient
            "CPTHOUT": str(config.map_dir / "bifprm.txt"),  # bifurcation channel table
        },
        "NRESTART": {
            "CRESTSTO": "",  # restart file
            "CRESTDIR": "./",  # restart directory
            "CVNREST": "restart",  # restart variable name
            "LRESTCDF": False,  # * true for netCDF restart file (double precision)
            "IFRQ_RST": 0,  # restart write frequency (1-24: hour, 0:end of run)
        },
        "NFORCE": {
            "LINPCDF": True,  # true for netCDF runoff
            "LINTERP": True,  # true for runoff interpolation using input matrix
            "CINPMAT": str(config.input_matrix_file),  # input matrix file name
            "DROFUNIT": config.drofunit,  # runoff unit conversion
            "CROFCDF": str(runoff_file),  # * netCDF input runoff file name
            "CVNROF": "Runoff",  # * netCDF input runoff variable name
            "SYEARIN": year,  # * netCDF input start year
            "SMONIN": 1,  # * netCDF input start month
            "SDAYIN": 1,  # * netCDF input start day
            "SHOURIN": config.shourin,  # * netCDF input start hour
        },
        "NOUTPUT": {
            "COUTDIR": "./",  # OUTPUT DIRECTORY
            "CVARSOUT": config.output_variables,  # Comma-separated list of output variables to save
            "COUTTAG": couttag,  # Output Tag Name for each experiment
            "LOUTCDF": config.loutcdf,  # true for netcdf output false for binary
            "NDLEVEL": 0,  # * NETCDF DEFLATION LEVEL (only used if LOUTCDF=True)
            "IFRQ_OUT": config.ifrq_out,  # output data write frequency (hour)
        },
    }
    

    # Ensure output directory exists
    namelist_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write namelist file (f90nml.write expects string path)
    f90nml.write(namelist_dict, str(namelist_path))


