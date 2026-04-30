"""
Convert a single CaMa-Flood binary output file to NetCDF using direct CLI args.

Usage example:
    python convert_cama_bin.py \
        --input-bin /path/to/maxdph2050.bin \
        --output-dir /path/to/converted \
        --diminfo-file /path/to/diminfo.txt \
        --start-year 2050 \
        --variable-name maxdph
"""

from __future__ import annotations

import argparse
from pathlib import Path

from src.cama_flood_api.binary_to_netcdf import binary_to_netcdf


DEFAULT_INPUT_BIN = Path("cama_flood/out/runoff_2050_single_year/maxdph2050.bin")
DEFAULT_OUTPUT_DIR = Path("data/cama_out")
DEFAULT_DIMINFO = Path("cama_flood/map/glb_15min/diminfo_test-1deg_grid.txt")
DEFAULT_START_YEAR = 2050
DEFAULT_VARIABLE_NAME = "maxdph"
DEFAULT_OUTPUT_FREQUENCY_HOURS = 24


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert one CaMa-Flood .bin file to NetCDF (.nc)."
    )
    parser.add_argument(
        "--input-bin",
        type=Path,
        default=DEFAULT_INPUT_BIN,
        help=(
            "Path to input CaMa-Flood binary file (.bin). "
            f"Default: {DEFAULT_INPUT_BIN}"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write converted NetCDF file. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--diminfo-file",
        type=Path,
        default=DEFAULT_DIMINFO,
        help=(
            "Path to diminfo file used for grid dimensions. "
            f"Default: {DEFAULT_DIMINFO}"
        ),
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help="Start year corresponding to the binary file time axis.",
    )
    parser.add_argument(
        "--variable-name",
        default=DEFAULT_VARIABLE_NAME,
        help=(
            "Variable name stored in NetCDF "
            f"(default: {DEFAULT_VARIABLE_NAME})."
        ),
    )
    parser.add_argument(
        "--output-frequency-hours",
        type=int,
        default=DEFAULT_OUTPUT_FREQUENCY_HOURS,
        help=f"Output frequency in hours (default: {DEFAULT_OUTPUT_FREQUENCY_HOURS}).",
    )
    return parser.parse_args()


def infer_variable_name(input_bin: Path) -> str:
    stem = input_bin.stem
    if len(stem) > 4 and stem[-4:].isdigit():
        return stem[:-4]
    return stem


def main() -> None:
    args = parse_args()
    input_bin = args.input_bin.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    diminfo_file = args.diminfo_file.expanduser().resolve()

    if not input_bin.exists():
        raise FileNotFoundError(f"Input binary file not found: {input_bin}")
    if not diminfo_file.exists():
        # Fallback for common case when map directory changed name but contains diminfo.
        candidates = list(Path("cama_flood").glob("**/diminfo*.txt"))
        if len(candidates) == 1:
            diminfo_file = candidates[0].resolve()
            print(f"Diminfo not found at given path, using discovered file: {diminfo_file}")
        else:
            raise FileNotFoundError(f"Diminfo file not found: {diminfo_file}")

    output_dir.mkdir(parents=True, exist_ok=True)
    variable_name = args.variable_name or infer_variable_name(input_bin)
    output_file = output_dir / f"o_{input_bin.stem}.nc"

    binary_to_netcdf(
        binary_file=input_bin,
        output_file=output_file,
        diminfo_file=diminfo_file,
        variable_name=variable_name,
        start_year=args.start_year,
        output_frequency_hours=args.output_frequency_hours,
    )

    print(f"Converted: {input_bin}")
    print(f"Wrote: {output_file}")


if __name__ == "__main__":
    main()
