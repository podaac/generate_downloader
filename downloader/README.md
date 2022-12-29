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

Arguments:
1.	list_directory
2.	job_index
3.	processing_level
4.	separator_character
5.	processing_type
6.	top_level_output_directory
7.	num_files_to_download
8.	sleep_time_in_between_files
9.	move_filelist_file_when_done
10.	perform_checksum_flag
11.	test_run_flag

MODIS A: 
`docker run --name gen-test -v /downloader/input:/data/input -v /downloader/logs:/data/logs -v /downloader/output:/data/output -v /downloader/scratch:/data/scratch downloader:0.1 /data/lists 0 L2 SPACE MODIS_A /data/output 5 1 yes yess`

MODIS T: 
`docker run --name gen-test -v /downloader/input:/data/input -v /downloader/logs:/data/logs -v /downloader/output:/data/output -v /downloader/scratch:/data/scratch downloader:0.1 /data/lists 0 L2 SPACE MODIS_T /data/output 5 1 yes yes`

VIIRS: 
`docker run --name gen-test -v /downloader/input:/data/input -v /downloader/logs:/data/logs -v /downloader/output:/data/output -v /downloader/scratch:/data/scratch downloader:0.1 /data/lists 0 L2 SPACE VIIRS /data/output 5 1 yes yes`

Please note that in order for the commands to execute the `/downloader/` directories will need to point to actual directories on the system.

## aws infrastructure

The downloader includes the following AWS services:
- AWS Batch job definition.
- CloudWatch log group.
- Elastic Container Registry repository.

## terraform 

Deploys AWS infrastructure and stores state in an S3 backend using a DynamoDB table for locking.

To deploy:
1. Edit `terraform.tfvars` for environment to deploy to.
2. Edit `terraform_conf/backed-{prefix}.conf` for environment deploy.
3. Initialize terraform: `terraform init -backend-config=terraform_conf/backend-{prefix}.conf`
4. Plan terraform modifications: `terraform plan -out=tfplan`
5. Apply terraform modifications: `terraform apply tfplan`

`{prefix}` is the account or environment name.