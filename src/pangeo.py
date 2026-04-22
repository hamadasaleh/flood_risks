import pprint
import webbrowser
import cf_xarray as cfxr
import dask
import intake
from loguru import logger
import xarray as xr
import xesmf as xe
from dask.distributed import Client
from tqdm.notebook import tqdm
from xmip.preprocessing import combined_preprocessing
from pathlib import Path


def get_cmip6_data_from_pangeo_api(
    query: str, store_dir: Path
) -> None:
    for query in [query]: #TODO
        # queries_pbar.set_postfix(climate_var=query["variable_id"])
        pprint.pp(query)
        cat = col.search(**query)

        with dask.config.set(**{"array.slicing.split_large_chunks": False}): #True
            clim_dset_dict = cat.to_dataset_dict(
                zarr_kwargs=z_kwargs, preprocess=combined_preprocessing
            )
            clim_pbar = tqdm(
                clim_dset_dict.keys(), desc="Processing model-experiment-variable"
            )
            for clim_key in clim_pbar:
                clim_pbar.set_postfix({"cmip6_setting": clim_key})
                ds_clim = clim_dset_dict[clim_key].squeeze()
                # Fetch data up to year 2100 (some models go to year 2300)
                ds_clim = ds_clim.sel(time=slice("1950-01-01T00:00:00", "2100-12-31T00:00:00"))

                # Drop unnecessary dims and variables after selection
                ds_clim = (
                    ds_clim.drop_vars(
                        ["member_id", "dcpp_init_year", "lon_bounds", "lat_bounds"]
                    )
                    .reset_coords(["time_bounds", "lat_verticies", "lon_verticies"])
                    .rename(
                        {
                            "lat_verticies": "lat_vertices",
                            "lon_verticies": "lon_vertices",
                        }
                    )
                )
                ds_clim["lon_vertices"] = ds_clim["lon_vertices"].chunk(
                    {"vertex": -1}
                )
                ds_clim["lat_vertices"] = ds_clim["lat_vertices"].chunk(
                    {"vertex": -1}
                )
                # Get the bounds variable and convert them to "vertices" format
                # Order=none, means that we do not know if the bounds are listed clockwise or counterclockwise, so we ask cf_xarray to try both.
                lat_corners = cfxr.bounds_to_vertices(
                    ds_clim.lat_vertices, "vertex", order=None
                )
                lon_corners = cfxr.bounds_to_vertices(
                    ds_clim.lon_vertices, "vertex", order=None
                )
                ds_clim = ds_clim.assign(lon_b=lon_corners, lat_b=lat_corners)
                ds_clim = ds_clim.drop_vars(["lon_vertices", "lat_vertices"])
                ds_clim = ds_clim.rename({"y_vertices": "y_b", "x_vertices": "x_b"})

                # Assign cf-compliant bounds for regridding
                ds_clim["lon"].attrs["bounds"] = "lon_b"
                ds_clim["lat"].attrs["bounds"] = "lat_b"
                print(ds_clim.cf)
                # Define an output grid (0.25° lat x 0.25° lon)
                ds_out = xe.util.grid_global(0.25, 0.25)

                # Print output grid
                print(ds_out)
                # Rechunk before regridding, to avoid the entire dataset being loaded into memory
                # ds_clim = ds_clim.chunk({"time": 10, "y": -1, "x": -1}) #TODO: check why this chunking no longer applies
                ds_clim["mrro"] = ds_clim["mrro"].chunk({"time": 10, "y": -1, "x": -1})

                # Create regridder (using first-order conservative method)
                regridder = xe.Regridder(
                    ds_clim, ds_out, "conservative", ignore_degenerate=True
                )

                ds_out = regridder(ds_clim)
                # Rechunk on time dimension before storing to zarr
                ds_out = ds_out.chunk({"time": 1, "y": -1, "x": -1})
                source_id = clim_key.split(".")[2]
                experiment_id = clim_key.split(".")[3]
                ds_out.to_zarr(
                    Path(store_dir).with_suffix(".zarr") / f"{source_id}/{experiment_id}",
                    mode="a",
                    zarr_format=2
                )


def generate_cmip6_queries(
    source_ids: list[str],
    experiment_ids: list[str],
    table_variables: dict[str, list[str]],
    member_id: str = "r1i1p1f1",
    grid_label: list[str] = ["gn"],
) -> list[dict]:
    """
    Generate a list of CMIP6 query dictionaries for combinations of tables and variables.

    Parameters
    ----------
    source_ids : list[str]
        List of CMIP6 model identifiers (source_id)
    experiment_ids : list[str]
        List of CMIP6 experiment identifiers (e.g., historical, ssp126)
    table_variables : dict[str, list[str]]
        Dictionary mapping table_id to list of variable_ids
    member_id : str, optional
        Member identifier, by default "r1i1p1f1"
    grid_label : list[str], optional
        Grid labels to query, by default ['gn'] (native grid)

    Returns
    -------
    list[dict]
        List of query dictionaries for all combinations
    """
    query_list = []

    # Create queries for each table and variable combination
    for table, variables in table_variables.items():
        for variable in variables:
            query = {
                "experiment_id": experiment_ids,
                "table_id": table,
                "source_id": source_ids,
                "variable_id": variable,
                "member_id": member_id,
                "grid_label": grid_label,
                "institution_id": "CSIRO-ARCCSS" # TODO: filter on institution to pick same as original implementation
            }
            query_list.append(query)



    # Display a sample query for verification
    if query_list:
        print("\nSample query:")
        pprint.pp(query_list[0])

    return query_list


if __name__ == "__main__":
    # Start Dask Client
    client = Client(n_workers=1, threads_per_worker=4, memory_limit="4GB")
    print(f"Dask dashboard: {client.dashboard_link}")

    logger.info(f"Fetching CMIP6 climate data from pangeo API")
    logger.info(f"Catalog available at this url: {'https://storage.googleapis.com/cmip6/pangeo-cmip6.csv'}")
    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    col = intake.open_esm_datastore(url)
    z_kwargs = {"consolidated": True, "decode_times": True, "use_cftime": True}
    query = {
        "institution_id": "BCC",
        "source_id": "BCC-CSM2-MR",
        "experiment_id": "ssp585", #"historical",
        "member_id": "r1i1p1f1",
        "table_id": "day",
        "variable_id": "mrro",
        "grid_label": "gn"

    }
    store_dir = "./data/cmip6_data"
    print(f"Save dir: {store_dir}")
    get_cmip6_data_from_pangeo_api(
        query=query, store_dir=store_dir
    )