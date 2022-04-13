"""
Download raw PRISM files from FTP server, preprocess and combine to zarr store.

Use from CLI like:

python download_prism.py \
  --firstyear 1999 \
  --lastyear 2015 \
  --variable "tmean" \
  --clipbox="minlon=-125.0,minlat=32.0,maxlon=-114.0,maxlat=43.0" \
  --outzarr "prism-tmean-1999-2015.zarr"
"""


from contextlib import contextmanager
from datetime import datetime
from functools import cache
from itertools import chain
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, Union, Sequence, Mapping, Iterable

import fsspec
from retry.api import retry_call
import rioxarray as rxr
import xarray as xr


logger = logging.getLogger(__name__)


class ZipBilFileError(Exception):
    """
    Zip archive did not have one .bil file.
    """

    pass


def _datetime_from_prism_flname(p: Path) -> datetime:
    return datetime.strptime(p.stem.split("_")[-2], "%Y%m%d")


def _dump_zippedbil(ofs: fsspec.core.OpenFiles, dumpdir: Path) -> Path:
    """
    Decompress .bil zip archive to local directory, return .bil path

    Raises
    ------
    ZipBilFileError : If one .bil file is not found in the zip archive.
    """
    # Also need to keep track of the actul .bil file for when we read all this.
    bil_path: Optional[Path] = None

    for fl in ofs:
        p = Path(fl.name)
        outpath = dumpdir.joinpath(p.name)
        with open(outpath, mode="wb") as outfl:
            outfl.write(fl.read())

        # Check if this file is a .bil. There should only be one .bil file in zip archive.
        if p.suffix == ".bil":
            if bil_path is not None:
                raise ZipBilFileError(
                    f"zip archive contains multiple .bil files: {bil_path}, {outpath}"
                )
            bil_path = outpath

    if bil_path is None:
        raise ZipBilFileError(f"zip archive did not have a .bil file")

    return bil_path


@contextmanager
def unpacked_prismzip_bil(url: str, fs: fsspec.filesystem) -> Path:
    """
    Context manager to download, unpack an ESRI .bil file from a Zip archive.

    Note that this uncompresses the Zip at the URL into a local directory because
    the goal is to read the archived .bil file. Reading the .bil file
    requires all the additional archived files to be decompressed and in the
    same directory as the .bil.

    Raises
    ------
    ZipBilFileError : If one .bil file is not found in the zip archive.
    """
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        # Download zip, then unzip. More reliable than chaining these together.
        tmpzip_path = tmpdir.joinpath(Path(url).name)

        retry_call(
            fs.get_file,
            fkwargs={"rpath": url, "lpath": tmpzip_path},
            exceptions=(TimeoutError,),
            tries=3,
            delay=30,
            backoff=2,
            logger=logger,
        )

        with fsspec.open_files(f"zip://*::{tmpzip_path}") as zipped_files:
            # This is creating problems. Getting EOFErrors after ****0110. Feels
            # like a connections is being left open or something.
            # Errors even if refactor as context manager.
            # zipped_files = fsspec.open_files(
            #     f"zip://*::simplecache::{url}",
            #     simplecache={"cache_storage": cachepath}
            # )
            target_bil_path = _dump_zippedbil(
                zipped_files,
                dumpdir=Path(tmpdir),
            )
            yield target_bil_path


@cache
def get_prism_daily_urls(
    year: Union[str, int],
    *,
    fs: fsspec.filesystem,
    variable: str,
    stability: str = "stable",
    scale: str = "4km",
    version: str = "D2",
) -> Iterable[str]:
    """
    Glob available URLs to daily PRISM Zip archives in a a given year

    We do this for a single year because daily PRISM Zips are stored in annual
    directories on the remote server.
    """
    target_file_glob = f"/daily/{variable}/{year}/PRISM_{variable}_{stability}_{scale}{version}_{year}*_bil.zip"
    return fs.glob(target_file_glob)


def preprocess_bil_dataarray(
    da: xr.DataArray,
    minlat: float = 32.0,
    minlon: float = -125.0,
    maxlat: float = 43.0,
    maxlon: float = -114.0,
    project_epsg: Optional[str] = "4326",
) -> xr.DataArray:
    """
    Clean, standardize input DataArray. Assumes it has `source_url` in attrs.
    """
    # Not reprojecting because WGS84 and NAD83 are likely going to have a
    # difference of a couple meters.
    if project_epsg is not None:
        da = da.rio.reproject(f"EPSG:{project_epsg}")

    # Generous box over California. Clipping too close has created an issue
    # when we apply population segment weights and aggregate the data to
    # census tracts.
    da = da.rio.clip_box(minx=minlon, miny=minlat, maxx=maxlon, maxy=maxlat)

    # Drop unused dimension/coords. This causes data to loose its CRS so
    # cannot do referenced spatial stuff after this.
    da = da.drop("spatial_ref").squeeze(drop=True)

    da = da.rename({"x": "lon", "y": "lat"})

    # Add time dim from source data URL path.
    source_url = Path(da.attrs["source_url"])
    da = da.expand_dims("time").assign_coords(
        time=("time", [_datetime_from_prism_flname(source_url)])
    )
    return da


def main(
    years: Sequence[int],
    *,
    variable: str,
    outpath: str,
    version: str,
    scale: str,
    stability: str,
    host: str = "ftp.prism.oregonstate.edu",
    protocol: str = "ftp",
    preprocess_kwargs: Optional[Mapping] = None,
) -> None:
    """
    Download and process years of daily PRISM data, output Zarr Store
    """
    fs = fsspec.filesystem(protocol=protocol, host=host, timeout=300)

    if preprocess_kwargs is None:
        preprocess_kwargs = {}

    prism_kwargs = {
        "variable": variable,
        "scale": scale,
        "version": version,
        "stability": stability,
    }

    all_urls = tuple(
        chain.from_iterable(
            (get_prism_daily_urls(yr, fs=fs, **prism_kwargs) for yr in years)
        )
    )
    n = len(all_urls)
    logger.info(f"found {n=} files to process")

    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        # Process all of the daily BIL files, outputting cleaned intermediate annual
        # files to a single local directory...
        daily_paths = []
        for url in all_urls:
            with unpacked_prismzip_bil(url, fs=fs) as bil_path:
                logger.info(f"unpacked {protocol}://{host}{url}")
                logger.debug(f"processing {bil_path}")

                da = rxr.open_rasterio(bil_path)
                da.name = variable
                da.attrs["source_url"] = str(url)

                # We preprocess these now rather than in open_mfdataset below
                # because this step includes spatial clipping, reducing the
                # volume of intermediate files stored to disk.
                da = preprocess_bil_dataarray(da, **preprocess_kwargs)

                daily_outpath = tmpdir.joinpath(bil_path.name).with_suffix(".nc")
                da.to_dataset().to_netcdf(daily_outpath)
                daily_paths.append(daily_outpath)
                logger.debug(f"{daily_outpath=}")

        logger.info(f"combining annual files to output Zarr store")
        ds = xr.open_mfdataset(
            daily_paths,
            parallel=True,
        )
        print(ds)  # DEBUG
        # `ds` must be written to disk or loaded to memory before the
        # func returns. Otherwise this conflicts with tmp directory cleanup
        # and input data is lost.
        ds.to_zarr(outpath)
        logger.info(f"output written to {outpath}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--firstyear", type=int)
    parser.add_argument("--lastyear", type=int)
    parser.add_argument("--variable", type=str)
    parser.add_argument("--outzarr", type=str)
    parser.add_argument("--version", type=str, default="D2")
    parser.add_argument("--scale", type=str, default="4km")
    parser.add_argument("--stability", type=str, default="stable")
    parser.add_argument(
        "--clipbox",
        type=str,
        default="minlon=-125.0,minlat=32.0,maxlon=-114.0,maxlat=43.0",
    )
    parser.add_argument("--epsg", type=str, default="4326")
    parser.add_argument("--host", type=str, default="ftp.prism.oregonstate.edu")
    parser.add_argument("--protocol", type=str, default="ftp")
    parser.add_argument("--loglevel", type=str, default="info")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=args.loglevel.upper()
    )

    logger.info(f"starting work")
    logger.debug(f"starting work with {args=}")

    preprocess_kwargs = {
        c.split("=")[0]: float(c.split("=")[1]) for c in args.clipbox.split(",")
    }

    epsg_arg = args.epsg
    if args.epsg.lower() == "none":
        epsg_arg = None
    preprocess_kwargs["project_epsg"] = epsg_arg

    main(
        range(args.firstyear, args.lastyear + 1),
        variable=args.variable,
        outpath=args.outzarr,
        version=args.version,
        scale=args.scale,
        stability=args.stability,
        host=args.host,
        protocol=args.protocol,
        preprocess_kwargs=preprocess_kwargs,
    )
    logger.info("done")
