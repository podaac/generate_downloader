#!/bin/csh
#
#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$
# DO NOT EDIT THE LINE ABOVE - IT IS AUTOMATICALLY GENERATED BY CM
#
#
# This is the C-shell wrapper to download the VIIRS/MODIS Level 2/ Level 3, and Aquarius NetCDF files from OBPG due to the "Recent" interface disappearing.
#
# It will usually be ran as part of a crontab.  Note that this script makes many requests to OBPG and they have
# a limit to 30 requests per minute per machine.  The variable sleep_time_in_between_files may have to be adjusted if there are errors from the Python script. 
#
# The log files created will be in directory $HOME/logs with the extension .log
#
# The list_directory parameter is the directory of download lists created by the create_generic_download_list.py script.
#
################################################################################################################################################################

# Set the environments.
source /app/config/downloader_config    # NET edit. (Docker container)
set module = startup_generic_downloader.csh

# Get the input.
if ($# < 9) then
echo $#
    echo "startup_generic_downloader:ERROR, You must specify at least 10 arguments: list_directory file_list_to_download job_index separator_character processing_type top_level_output_directory num_files_to_download sleep_time_in_between_files move_filelist_file_when_done"
    echo "startup_generic_downloader:Usage:"
    echo ""
    echo "    source startup_generic_downloader.csh ~/scratch/viirs_level2_download_list/list.txt    0 L2 SPACE VIIRS    /data/dev/scratch/qchau/IO/data 1    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level2_download_list/list.txt    0 L2 SPACE MODIS_A  /data/dev/scratch/qchau/IO/data 1    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level2_download_list/list.txt    0 L2 SPACE MODIS_T  /data/dev/scratch/qchau/IO/data 1    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level3_download_list/list.txt    0 L3 SPACE MODIS_A  /data/dev/scratch/qchau/IO/data 1    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level3_download_list/list.txt    0 L3 SPACE MODIS_T  /data/dev/scratch/qchau/IO/data 1    0 no  no"
    echo ""
    echo "    source startup_generic_downloader.csh ~/scratch/viirs_level2_download_list/list.txt    0 L2 SPACE VIIRS    /data/dev/scratch/qchau/IO/data 5    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level2_download_list/list.txt    0 L2 SPACE MODIS_A  /data/dev/scratch/qchau/IO/data 5    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level2_download_list/list.txt    0 L2 SPACE MODIS_T  /data/dev/scratch/qchau/IO/data 5    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level3_download_list/list.txt    0 L3 SPACE MODIS_T  /data/dev/scratch/qchau/IO/data 5    0 no  no"
    echo "    source startup_generic_downloader.csh ~/scratch/modis_level3_download_list/list.txt    0 L3 SPACE MODIS_T  /data/dev/scratch/qchau/IO/data 5    0 no  no"
    exit
endif

# The parameters for startup_generic_downloader.csh are:
#
#  1 = list_directory
#  2 = job_index 
#  3 = processing_level
#  4 = separator_character
#  5 = processing_type
#  6 = top_level_output_directory
#  7 = num_files_to_download
#  8 = sleep_time_in_between_files
#  9 = move_filelist_file_when_done
# 10 = perform_checksum_flag
# 11 = test_run_flag 

set list_directory               = $1
set list_name                    = $2
set job_index                    = $3
set processing_level             = $4
set separator_character          = $5
set processing_type              = $6
set top_level_output_directory   = $7
set num_files_to_download        = $8
set sleep_time_in_between_files  = $9
set move_filelist_file_when_done = $10
set perform_checksum_flag        = $11
set test_run_flag                = $12

# Create the logs directory if it does not exist yet   # NET edit.
set logging_dir = `printenv | grep OBPG_DOWNLOADER_LOGGING | awk -F= '{print $2}'`
if (! -e $logging_dir) then    # NET edit.
    mkdir $logging_dir    # NET edit.
endif

# set log_top_level_directory = "$HOME/logs"
set log_top_level_directory = $logging_dir    # NET edit.

# Create a random number
set job_id_list = ($AWS_BATCH_JOB_ID:as/:/ /)
if(${#job_id_list} == 2) then
  setenv RANDOM_NUMBER "$job_id_list[1]-$job_id_list[2]"
else
  setenv RANDOM_NUMBER $AWS_BATCH_JOB_ID
endif
echo "RANDOM NUMBER: $RANDOM_NUMBER"

# Get today's date so we can name our log file.
# The format will be mm_dd_yy_HH_MM as in 09_19_12_16_02
# The touch command is to create an empty file if it does not exist yet.

# Note: This date is to be Pacific Time.
setenv TZ PST8PDT
set today_date = "`date '+%m_%d_%y_%H_%M'`_$RANDOM_NUMBER"    # Use random number to differentiate parallel runs

# Set this flag DOWNLOADER_UNIT_TEST so the python script get_unique_python_processes_on_system.py will run correctly.
setenv DOWNLOADER_UNIT_TEST false

# Add leading zeros to keep the file name's length constant.
set list_processed = 1
set digits_in_name = "000$list_processed";

# Name snippet
set scratch = `printenv | grep SCRATCH_AREA | awk -F= '{print $2}'`
if $processing_type == 'MODIS_A' then
    setenv JOB_DIRECTORY $scratch/modis_aqua_level2_download_processes
    set name_snippet = "modis_level2_aqua"
endif
if $processing_type == 'MODIS_T' then
    setenv JOB_DIRECTORY $scratch/modis_terra_level2_download_processes
    set name_snippet = "modis_terra"
endif
if $processing_type == 'VIIRS' then
    setenv JOB_DIRECTORY $scratch/viirs_level2_download_processes
    set name_snippet = "viirs_level2"
endif

# Log name
set downloader_log_name = "$log_top_level_directory/$name_snippet""_${processing_type}_downloader_output_${today_date}_list_${digits_in_name}.log" 
rm -f $downloader_log_name
touch $downloader_log_name

# Job directory and job name
if (! -e $JOB_DIRECTORY) then
    mkdir -p $JOB_DIRECTORY
    chmod 777 $JOB_DIRECTORY
endif
setenv JOB_NAME_ONLY "obpg_download_process_"$name_snippet"_${processing_type}_downloader_output_${today_date}_list_${digits_in_name}.txt"
setenv JOB_FULL_NAME $JOB_DIRECTORY/$JOB_NAME_ONLY
touch $JOB_FULL_NAME

# Retrieve file list name
if ($job_index == "-235") then
    set index = $AWS_BATCH_JOB_ARRAY_INDEX
else
    set index = $job_index
endif
set list_files = `cat $list_directory/$list_name | jq -r --argjson index $index '.[$index]'`
set file_list_to_download = $list_directory/$list_files

# Echo data about program
echo "Command line arguments:"
echo "    list_directory                $1"
echo "    list_name                     $2"
echo "    job_index:                    $3"
echo "    processing_level:             $4"
echo "    separator_character:          $5"
echo "    processing_type:              $6"
echo "    top_level_output_directory:   $7"
echo "    num_files_to_download:        $8"
echo "    sleep_time_in_between_files:  $9"
echo "    move_filelist_file_when_done: $10"
echo "    perform_checksum_flag:        $11"
echo "    test_run_flag:                $12"
echo ""
echo "Log file                          $downloader_log_name"
echo "Job name                          $JOB_FULL_NAME"
echo ""

if ($processing_type == "MODIS_A") then
    set dataset = "MODIS Aqua"
else if ($processing_type == "MODIS_T") then
    set dataset = "MODIS Terra"
else
    set dataset = $processing_type
endif
echo "$module - INFO: Dataset: $dataset"
echo "$module - INFO: Job identifier: $AWS_BATCH_JOB_ID"
echo "$module - INFO: Job index: $index"
echo "$module - INFO: JSON file: $list_name"
echo "$module - INFO: TXT file: $file_list_to_download"

# Download files in list
set python_exe = `printenv | grep PYTHON3_EXECUTABLE_PATH | awk -F= '{print $2}'` 
if ($test_run_flag == "true") then
    echo "$python_exe $OBPG_RUNENV_PYTHON_HOME/generic_level2_downloader.py $file_list_to_download $processing_level $separator_character $processing_type $top_level_output_directory $num_files_to_download $sleep_time_in_between_files $move_filelist_file_when_done $perform_checksum_flag $today_date $JOB_FULL_NAME $test_run_flag "
    $python_exe $OBPG_RUNENV_PYTHON_HOME/generic_level2_downloader.py $file_list_to_download $separator_character $processing_type $top_level_output_directory $num_files_to_download $sleep_time_in_between_files $move_filelist_file_when_done $perform_checksum_flag $today_date  $JOB_FULL_NAME $test_run_flag
else
    echo "$python_exe $OBPG_RUNENV_PYTHON_HOME/generic_level2_downloader.py $file_list_to_download $processing_level $separator_character $processing_type $top_level_output_directory $num_files_to_download $sleep_time_in_between_files $move_filelist_file_when_done $perform_checksum_flag $today_date $JOB_FULL_NAME $test_run_flag | tee $downloader_log_name"
    $python_exe $OBPG_RUNENV_PYTHON_HOME/generic_level2_downloader.py $file_list_to_download $processing_level $separator_character $processing_type $top_level_output_directory $num_files_to_download $sleep_time_in_between_files $move_filelist_file_when_done $perform_checksum_flag $today_date  $JOB_FULL_NAME $test_run_flag | tee $downloader_log_name
endif

# Get contents of error file indicator see if any errors were encountered in python script and exit with non-zero status
set job_id_list = ($AWS_BATCH_JOB_ID:as/:/ /)
if(${#job_id_list} == 2) then
  set error_filename = "error-$job_id_list[1]-$job_id_list[2].txt"
else
  set error_filename = "error-$AWS_BATCH_JOB_ID.txt"
endif
set error_file="$logging_dir/$error_filename"
if ( -f "$error_file" ) then
    echo "ERROR FILE REMOVAL    $error_file"
    rm -rf $error_file    # Remove error file indicator
    echo "startup_generic_downloader_job_index.csh exiting with status of 1"
    exit(1)
endif

# Check for NetCDF: HDF error
set check=`$OBPG_RUNENV_PYTHON_HOME/check_netcdf_error.py $downloader_log_name`
if ($check == "error") then
    echo "startup_generic_downloader_job_index.csh exiting with status of 1"
    exit(1)
endif
