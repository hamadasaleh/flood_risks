from __future__ import annotations

from pathlib import Path
import sys

import pytest

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

xr = pytest.importorskip("xarray")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cama_flood_api.config import CaMaFloodConfig  # noqa: E402


def load_config(config_path: Path) -> CaMaFloodConfig:
    with config_path.open("rb") as f:
        config_dict = tomllib.load(f)
    return CaMaFloodConfig(**config_dict)


@pytest.fixture(scope="session")
def config() -> CaMaFloodConfig:
    return load_config(PROJECT_ROOT / "config_2050.toml")


def test_runoff_file_exists_for_target_year(config: CaMaFloodConfig) -> None:
    runoff_file = config.runoff_dir / f"{config.runoff_prefix}{config.start_year}.nc"
    assert runoff_file.exists(), f"Missing runoff file: {runoff_file}"
    assert runoff_file.stat().st_size > 0, f"Empty runoff file: {runoff_file}"


def test_runoff_has_runoff_variable_and_nonempty_time(config: CaMaFloodConfig) -> None:
    runoff_file = config.runoff_dir / f"{config.runoff_prefix}{config.start_year}.nc"
    with xr.open_dataset(runoff_file) as ds:
        assert "Runoff" in ds.variables, "Expected variable 'Runoff' not found"
        assert "time" in ds.dims, "Expected time dimension not found"
        assert ds.sizes["time"] > 0, "Time dimension is empty"


def test_mapping_inputs_exist_and_nonempty(config: CaMaFloodConfig) -> None:
    assert config.dimension_info_file.exists(), (
        f"Missing dimension info file: {config.dimension_info_file}"
    )
    assert config.input_matrix_file.exists(), (
        f"Missing input matrix file: {config.input_matrix_file}"
    )
    assert config.dimension_info_file.stat().st_size > 0, (
        f"Empty dimension info file: {config.dimension_info_file}"
    )
    assert config.input_matrix_file.stat().st_size > 0, (
        f"Empty input matrix file: {config.input_matrix_file}"
    )
