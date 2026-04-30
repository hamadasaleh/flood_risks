from __future__ import annotations

from pathlib import Path
import sys

import pytest

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cama_flood_api.config import CaMaFloodConfig  # noqa: E402
from src.cama_flood_api.runner import CaMaFloodRunner  # noqa: E402


def load_config(config_path: Path) -> CaMaFloodConfig:
    with config_path.open("rb") as f:
        config_dict = tomllib.load(f)
    return CaMaFloodConfig(**config_dict)


@pytest.mark.slow
@pytest.mark.skipif(
    not (Path.cwd() / ".run_smoke_tests").exists(),
    reason="Create .run_smoke_tests in repo root to enable expensive smoke runs.",
)
def test_smoke_run_2050_creates_expected_outputs() -> None:
    config = load_config(PROJECT_ROOT / "config_2050.toml")
    with CaMaFloodRunner(config, show_progress=False) as runner:
        runner.run()

    output_dir = config.get_output_dir()
    year = config.start_year
    expected_vars = [v.strip() for v in config.output_variables.split(",") if v.strip()]

    for var_name in expected_vars:
        binary_output = output_dir / f"{var_name}{year}.bin"
        netcdf_output = output_dir / f"{var_name}{year}.nc"
        assert binary_output.exists() or netcdf_output.exists(), (
            f"Expected output for {var_name} in {year} not found in {output_dir}"
        )
