#!/app/env/bin/python3
#***************************************************************************
#
# Copyright 2017, by the California Institute of Technology. ALL
# RIGHTS RESERVED. United States Government Sponsorship acknowledged.
# Any commercial use must be negotiated with the Office of Technology
# Transfer at the California Institute of Technology.
#
# @version $Id$
#
#****************************************************************************
# Given a file containing a list of URIs, program will spawn a process to download the file to local disk.
#
# The URI is in the form of:
#
#    http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016230232400.L2_SNPP_SST3.nc
#    http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016230232400.L2_SNPP_SST.nc
#
# and the wget command will be used to perform the download.
#
# If the URI is just a file name, this script will preceed the URI with the string "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to pass along
# to the wget command.
#
# If the file contains comma or space separated values, this script can support it too.
# If the checksum is provided, it should be separated by a space or comma.  The checksum type is assumed to be SHA-1.
#
# The number of tries will be 45 times due to network hiccups and such.
# Based on the data source (the first character of the file), the output directory will be
#
#    VIIRSL2_SST_OBPG
#
# so the MODIS Level 2 Uncompressor (related to the MODIS Level 2 Combiner) can find them later.
#
# The list contains one name per line.
# Each line contains the URL to the file_search.cgi and the SHA-1 checksum:
#
#{lapinta}/home/qchau/sandbox/trunk/ghrsst-rdac/combine/src/main/python 175 % head -5  modis_aqua_filelist.txt.daily_2016_008_date_2016_01_08_sorted
#
#http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008000000.L2_LAC_OC.nc d2538d481de288cb0774e24b9458c59601b2cfe4
#http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008000000.L2_LAC_SST.nc feeb8e6fd758deb39152ec880a8bc960831d6fd8
#http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008000500.L2_LAC_OC.nc 78dd9c531a55ee6046d63aa0bced9f4b57352a48
#http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008000500.L2_LAC_SST.nc 3616781364088139c42d6c5cdc7e9f16924dc485
#http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008001000.L2_LAC_OC.nc 5a283c510818efb95ead16c013bdea5b1bde17cb

import datetime
import getopt
import hashlib
import os
import pathlib
import sys
import time
import urllib.request, urllib.error, urllib.parse

from time import strftime, gmtime

# Import user-defined modules.

from create_netrc import create_netrc, remove_netrc
from generic_level2_downloader_driver_historical_using_process import generic_level2_downloader_driver_historical_using_process;
from inspect_required_env_settings  import inspect_required_env_settings;

def generic_level2_downloader(i_filelist_name,
                              i_processing_level,
                              i_separator_character,
                              i_processing_type,
                              i_top_level_output_directory,
                              i_num_files_to_download,
                              i_sleep_time_in_between_files,
                              i_move_filelist_file_when_done,
                              i_perform_checksum_flag,
                              i_today_date,
                              i_job_full_name,
                              i_test_run_flag):

    debug_module = "generic_level2_downloader:";
    debug_mode   = 0;

    # Check to see if all the required settings are there.
    # Function will exit if something goes wrong.  We don't need to check for returned status.
    all_required_settings_are_ok = inspect_required_env_settings();

    o_download_driver_status = generic_level2_downloader_driver_historical_using_process(i_filelist_name,
                                                i_separator_character,
                                                i_processing_level,
                                                i_processing_type,
                                                i_top_level_output_directory,
                                                i_num_files_to_download,
                                                i_sleep_time_in_between_files,
                                                i_move_filelist_file_when_done,
                                                i_perform_checksum_flag,
                                                i_today_date,
                                                i_job_full_name,
                                                i_test_run_flag);
    

def get_today_date():
    # Return today's date in 11_20_14_11_25
    #                        mm_dd_yy_hh_mm
    o_today_date = ""

    # Get the current year and day of year and create the subdirectory if it does not already exist, i.e. 2015/192 for August 11, 2015.
    os.environ["TZ"]="US/Pacific"
    time.tzset();
    localtime = time.localtime(time.time())
    this_year   = str(localtime.tm_year)[-2:];
    this_month  = str("%02d" % localtime.tm_mon);
    this_day    = str("%02d" % localtime.tm_mday);
    this_hour   = str("%02d" % localtime.tm_hour);
    this_minute = str("%02d" % localtime.tm_min);
    this_second = str("%02d" % localtime.tm_sec);

    o_today_date = this_year + "_" + this_month + "_" + this_day + "_" + this_hour + "_" + this_minute + "_" + this_second;
    return(o_today_date);

def validate_input(i_filelist_name,
                   i_processing_level,
                   i_separator_character,
                   i_processing_type,
                   i_top_level_output_directory,
                   i_num_files_to_download,
                   i_sleep_time_in_between_files,
                   i_move_filelist_file_when_done,
                   i_perform_checksum_flag,
                   i_today_date,
                   i_job_full_name,
                   i_test_run_flag):

    # Attempt to validate the input parameters.  Exit if any is found.

    g_debug_module = "generic_level2_downloader:";
    debug_mode   = 0;
    if (os.getenv('CRAWLER_SEARCH_DEBUG_FLAG','') == 'true'):
        debug_mode   = 1;

    error_found_flag = False;

    if not os.path.isfile(i_filelist_name):
        print(g_debug_module + "ERROR: Input file does not exist " + i_filelist_name);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Input file does exist " + i_filelist_name);
    if (i_processing_level not in ['L2','L3','L4']):
        print(g_debug_module + "ERROR: Not a valid value i_processing_level ",i_processing_level);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Is a valid value i_processing_level ",i_processing_level);
    if (i_separator_character not in ["COMMA","SPACE"]):
        print(g_debug_module + "ERROR: Not a valid value i_separator_character ",i_separator_character);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Is a valid value i_separator_character ",i_separator_character);
    if (i_processing_type not in ['VIIRS','MODIS_A','MODIS_T','AQUARIUS']):
        print(g_debug_module + "ERROR: Not a valid value i_processing_type",i_processing_type);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Is a valid value i_processing_type",i_processing_type);
    if not os.path.isdir(i_top_level_output_directory):
        print(g_debug_module + "ERROR: Directory does not exist " + i_top_level_output_directory);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Directory does indeed exist " + i_top_level_output_directory);
    if i_num_files_to_download < 0:
        print(g_debug_module + "ERROR: Value of i_num_files_to_download must be 0 or positive value ", i_num_files_to_download);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Value of i_num_files_to_download is valid", i_num_files_to_download);
    if (i_sleep_time_in_between_files < 0):
        print(g_debug_module + "ERROR: Value of i_sleep_time_in_between_files must be a zero or positive number ", i_sleep_time_in_between_files);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Value of i_sleep_time_in_between_files is valid", i_sleep_time_in_between_files);
    if (i_move_filelist_file_when_done not in ["yes","no"]):
        print(g_debug_module + "ERROR: Not a valid value i_move_filelist_file_when_done ",i_move_filelist_file_when_done);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Is a valid value i_move_filelist_file_when_done ",i_move_filelist_file_when_done);
    if (i_perform_checksum_flag not in ["yes","no"]):
        print(g_debug_module + "ERROR: Not a valid value i_perform_checksum_flag",i_perform_checksum_flag);
        error_found_flag = True;
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Is a valid value i_perform_checksum_flag",i_perform_checksum_flag);

    if (error_found_flag):
        print(g_debug_module + "ERROR: Cannot continue.  Error(s) found in parameters.");
        raise FileNotFoundError # Raise file not found error
    else:
      if (debug_mode):
        print(g_debug_module + "INFO: Will continue.  No errors found in parameters.");

    return;

def write_out_error_file():
    """Write out text file if errors are encountered.
    
    This will alert the calling script that an error has occured so that it may
    set the program exit code appropriately.
    """
    
    log_dir = pathlib.Path(os.getenv('OBPG_DOWNLOADER_LOGGING'))
    job_id = os.getenv('AWS_BATCH_JOB_ID')
    child_job = job_id.split(':')
    if len(child_job) == 2:
        job_id = f"{child_job[0]}-{child_job[1]}"
    error_file = log_dir.joinpath(f"error-{job_id}.txt")
    print(f"generic_level2_downloader, ERROR FILE: {error_file}.")
    with open(error_file, 'w') as fh:
        fh.write("error")

if __name__ == "__main__":
    # python generic_level2_downloader.py i_filelist_name i_processing_level i_separator_character i_processing_type i_top_level_output_directory i_num_files_to_download i_sleep_time_in_between_files i_move_filelist_file_when_done i_perform_checksum_flag
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/viirs_filelist.txt.daily_2016_293_date_2016_10_19 L2 SPACE VIIRS /data/dev/scratch/qchau/IO/data 1 0 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/modis_aqua_filelist.txt.daily_2016_008_date_2016_01_08  L2 SPACE MODIS_A /data/dev/scratch/qchau/IO/data 30 2 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/modis_aqua_filelist.txt.daily_2016_008_date_2016_01_08  L2 SPACE MODIS_A /data/dev/scratch/qchau/IO/data  1 0 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/modis_aqua_filelist.txt.daily_2016_008_date_2016_01_08_two_names_only  L2 SPACE MODIS_A /data/dev/scratch/qchau/IO/data  1 0 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/modis_terra_filelist.txt.daily_2016_344_date_2016_12_09                L2 SPACE MODIS_T /data/dev/scratch/qchau/IO/data  1 0 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/viirs_filelist.txt.daily_2016_293_date_2016_10_19       L2 SPACE VIIRS /data/dev/scratch/qchau/IO/data 3  0 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/viirs_filelist.txt.daily_2016_293_date_2016_10_19       L2 SPACE VIIRS /data/dev/scratch/qchau/IO/data 1  1 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/aquarius_filelist.txt.daily_2014_215_date_2014_08_03    L2 SPACE AQUARIUS /data/dev/scratch/qchau/IO/data 1  1 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/aquarius_filelist.txt.daily_2015_158_date_2015_06_07_created_time_16_50_28_1478303428155 L2 SPACE AQUARIUS /data/dev/scratch/qchau/IO/data 1  1 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/aquarius_filelist.txt.daily_empty L2 SPACE AQUARIUS /data/dev/scratch/qchau/IO/data 1  1 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/aquarius_filelist.txt.daily_2015_158_date_2015_06_07_created_time_16_51_08_1478303468657  L2 SPACE AQUARIUS /data/dev/scratch/qchau/IO/data 16 1 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/aquarius_filelist.txt.daily_2015_158_date_2015_06_07_created_time_16_51_08_1478303468657  L2 SPACE AQUARIUS /data/dev/scratch/qchau/IO/data 2 1 no no
    # python generic_level2_downloader.py $OBPG_RUNENV_RESOURCES_HOME/aquarius_filelist.txt.daily_with_blank_lines L2 SPACE AQUARIUS /data/dev/scratch/qchau/IO/data 3 1 no no

    debug_module = "generic_level2_downloader:";
    debug_mode   = 0;
    # Get the input parameters.

    i_filelist_name               = sys.argv[1];  # Containing a list of names to download.
    i_processing_level            = sys.argv[2];  # Processing Level {L2,L3}
    i_separator_character         = sys.argv[3];  # Separator character in file list.  Possible values are {COMMA, SPACE}.
    i_processing_type             = sys.argv[4];  # Which directory to move the MODIS Level 2 files to.  MODIS_A type moves downloaded file to MODIS_AQUA_L2_SST_OBPG.  MODIS_T type moves downloaded file to MODIS_TERRA_L2_SST_OBPG directory.
    i_top_level_output_directory  = sys.argv[5];  # Top level directory to directory to download file to.
    i_num_files_to_download       = int(sys.argv[6]);  # Batch size.  Useful in testing when the file list is large.
    i_sleep_time_in_between_files = int(sys.argv[7]);  # Sleep time in between files.  OBPG has limitation on how many requests can be made in 30 seconds.  Start with 0 and bump to 10 if they time out.
    i_move_filelist_file_when_done = sys.argv[8]; # Move file list file when done for safe keeping for some time.  A separate process can delete them after a certain number of days.
    i_perform_checksum_flag        = sys.argv[9]; # Perform the checksum check or not.

    # Additional parameters to deal with running jobs

    i_today_date    = ""; # Used so the files can be moved into a distinct directory.
    i_job_full_name = ""; # This file can be removed once the job is finished.
    i_test_run_flag = ""; # Flag used by developer only.

    num_args_to_check = 11;
    if (len(sys.argv) >= num_args_to_check):
        i_today_date = sys.argv[num_args_to_check-1]; # Used so the files can be moved into a distinct directory.

    # If did not receive i_today_date from command line, we can get today's date via the get_today_date() function.
    if (i_today_date == ""):
        i_today_date = get_today_date();

    num_args_to_check = 12;
    if (len(sys.argv) >= num_args_to_check):
        i_job_full_name = sys.argv[num_args_to_check-1]; # This job file can be removed once the job is finished.

    num_args_to_check = 13;
    if (len(sys.argv) >= num_args_to_check):
        i_test_run_flag = sys.argv[num_args_to_check-1]; # Flag used by developer only.

    if (debug_mode):
        print(debug_module + "len(sys.argv)",len(sys.argv));
        print(debug_module + "i_filelist_name",i_filelist_name);
        print(debug_module + "i_separator_character",i_separator_character);
        print(debug_module + "i_processing_type",i_processing_type);
        print(debug_module + "i_top_level_output_directory",i_top_level_output_directory);
        print(debug_module + "i_num_files_to_download",i_num_files_to_download);
        print(debug_module + "i_sleep_time_in_between_files",i_sleep_time_in_between_files);
        print(debug_module + "i_move_filelist_file_when_done",i_move_filelist_file_when_done);
        print(debug_module + "i_perform_checksum_flag",i_perform_checksum_flag);
        print(debug_module + "i_today_date",i_today_date);
        print(debug_module + "i_job_full_name",i_job_full_name);
        print(debug_module + "i_test_run_flag",i_test_run_flag);

    try:
        create_netrc()
        validate_input(i_filelist_name,
                    i_processing_level,
                    i_separator_character,
                    i_processing_type,
                    i_top_level_output_directory,
                    i_num_files_to_download,
                    i_sleep_time_in_between_files,
                    i_move_filelist_file_when_done,
                    i_perform_checksum_flag,
                    i_today_date,
                    i_job_full_name,
                    i_test_run_flag);

        # The input are good, we can proceed with the download.

        generic_level2_downloader(i_filelist_name,
                                i_processing_level,
                                i_separator_character,
                                i_processing_type,
                                i_top_level_output_directory,
                                i_num_files_to_download,
                                i_sleep_time_in_between_files,
                                i_move_filelist_file_when_done,
                                i_perform_checksum_flag,
                                i_today_date,
                                i_job_full_name,
                                i_test_run_flag);
        remove_netrc()
    except Exception as e:
        write_out_error_file()
        print("ERROR encountered...")
        print(type(e))
        print(e)
        if pathlib.Path(os.getenv("NETRC_DIR")).joinpath(".netrc").exists(): remove_netrc()
        print("Exiting with exit code 1.")
        sys.exit(1)
    else:
        sys.exit(0) # Return a value of 0 means good.
