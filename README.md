[![Build, push image](https://github.com/brews/nastyprisms/actions/workflows/buildpush.yaml/badge.svg)](https://github.com/brews/nastyprisms/actions/workflows/buildpush.yaml)

# nastyprisms

A simple containerized command-line application to download and process daily PRISM climate fields into a single Zarr Store.

## Example

This script is usually run as from container as part of a larger orchestrated workflow on Kubernetes, but you can also run this locally. For example, this downloads a range of years and clips to a bounding box around California after reprojecting the climate data to [EPSG:4326](https://epsg.io/4326):

```shell
docker pull ghcr.io/brews/nastyprisms:0.1.0
docker run ghcr.io/brews/nastyprisms:0.1.0 \
  --firstyear 1999 \
  --lastyear 2000 \
  --variable "tmean" \
  --epsg "4326" \
  --clipbox="minlon=-125.0,minlat=32.0,maxlon=-114.0,maxlat=43.0" \
  --outzarr "gs://myscratchbucket/prism-tmean-1999-2000.zarr"
```

`./example-workflow.yaml` is an Argo Workflow using the `nastyprisms` container to download 3 separate variables in a way that balances reliable processing without beating the PRISM FTP server to death with requests.

## Installation

Grab the latest copy of the container with `docker pull ghcr.io/brews/nastyprisms:latest`. If you're tinkering with the source code the `conda` virtual environment specs used for container builds are in `environment.yaml`. We recommend you build your virtual environment with `conda` because this depends on several difficult-to-compile geospatial libraries.

## Support

Source code is available online at https://github.com/brews/nastyprisms. This software is Open Source and available under the Apache License, Version 2.0. Please file bugs at https://github.com/brews/nastyprisms/issues. Feel free to fork the code and file a merge-request with bug fixes and improvements. Or just fork the code for yourself.

"nastyprisms" is a reference to Dan Meth's Frederator video ["Pink Floyd's Syd Barrett Visits His Accountant"](https://www.youtube.com/watch?v=YMNWHLPSgBE). It's the only thing I could think of at the time that references "prism".
