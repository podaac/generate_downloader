# downloader

The downloader downloads files from the OBPG based on lists created by the download list creator component.

## pre-requisites to building

The following variables must be present in the user's environment:
- MACHINE
- LOGIN
- PASSWORD

The script `netrc_env_vars.sh` found in the top-level `generate` directory can be used to export these variables to the user's environment. They are used to create the `.netrc` file used by the OBPG download operations to download data. See this link for more information on the .netrc file: https://podaac.github.io/2022-SWOT-Ocean-Cloud-Workshop/prerequisites/02_NASA_Earthdata_Authentication.html

## build command

`docker build --build-arg MACHINE --build-arg LOGIN --build-arg PASSWORD --tag downloader:0.1 . `

## execute command

MODIS A: 
`docker run --name gen-test -v /downloader/input:/data/input -v /downloader/logs:/data/logs -v /downloader/output:/data/output -v /downloader/scratch:/data/scratch downloader:0.1 /data/input L2 SPACE MODIS_A /data/output 1000 1 1 yes yes`

MODIS T: 
`docker run --name gen-test -v /downloader/input:/data/input -v /downloader/logs:/data/logs -v /downloader/output:/data/output -v /downloader/scratch:/data/scratch downloader:0.1 /data/input L2 SPACE MODIS_T /data/output 1000 1 1 yes yes`

VIIRS: 
`docker run --name gen-test -v /downloader/input:/data/input -v /downloader/logs:/data/logs -v /downloader/output:/data/output -v /downloader/scratch:/data/scratch downloader:0.1 /data/input L2 SPACE VIIRS /data/output 1000 1 1 yes yes`

Please note that in order for the commands to execute the `/downloader/` directories will need to point to actual directories on the system.