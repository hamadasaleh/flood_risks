from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
import pytest

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cama_flood_api.binary_to_netcdf import read_binary_file, read_diminfo  # noqa: E402
from src.cama_flood_api.config import CaMaFloodConfig  # noqa: E402


def load_config(config_path: Path) -> CaMaFloodConfig:
    with config_path.open("rb") as f:
        config_dict = tomllib.load(f)
    return CaMaFloodConfig(**config_dict)


@pytest.fixture(scope="session")
def config() -> CaMaFloodConfig:
    return load_config(PROJECT_ROOT / "config_2050.toml")


@pytest.fixture(scope="session")
def maxdph_binary(config: CaMaFloodConfig) -> Path:
    output_dir = config.get_output_dir()
    path = output_dir / f"maxdph{config.start_year}.bin"
    assert path.exists(), f"Missing maxdph output file: {path}"
    assert path.stat().st_size > 0, f"Empty maxdph output file: {path}"
    return path


def test_maxdph_record_size_and_timestep_count(
    config: CaMaFloodConfig, maxdph_binary: Path
) -> None:
    nx, ny, *_ = read_diminfo(config.dimension_info_file)
    record_size = 4 * nx * ny  # float32
    file_size = maxdph_binary.stat().st_size
    assert file_size % record_size == 0, (
        f"File size ({file_size}) is not a multiple of record size ({record_size})"
    )
    n_timesteps = file_size // record_size
    assert n_timesteps > 0, "No timesteps found in maxdph output"


def test_maxdph_no_nan_or_negative_values(
    config: CaMaFloodConfig, maxdph_binary: Path
) -> None:
    nx, ny, *_ = read_diminfo(config.dimension_info_file)
    data = read_binary_file(maxdph_binary, nx=nx, ny=ny)
    assert data.size > 0, "maxdph data array is empty"
    assert np.isfinite(data).all(), "maxdph contains NaN or infinite values"
    assert (data >= 0).all(), "maxdph contains negative values"


def test_maxdph_not_all_zero(config: CaMaFloodConfig, maxdph_binary: Path) -> None:
    nx, ny, *_ = read_diminfo(config.dimension_info_file)
    data = read_binary_file(maxdph_binary, nx=nx, ny=ny)
    assert np.any(data > 0), "maxdph appears to be all zeros"
