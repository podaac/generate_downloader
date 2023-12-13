#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$
# DO NOT EDIT THE LINE ABOVE - IT IS AUTOMATICALLY GENERATED BY CM

# Given a file name and some file attributes, this function will perform the downloading of file and move the file to its final destination.
#
# The URI is in the form of:
#
#     https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2013048142500.L2_LAC_OC.bz2
#
# and the wget or curl command will be used to perform the download.
#
# If the URI is just a file name, this script will preceed the URI with the string "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to pass along
# to the wget command.
#
# If the file contains comma or space separated values, this script can support it too.
# If the checksum is provided, it should be separated by a space or comma.  The checksum type is assumed to be SHA-1.
#
# The number of tries will be 45 times due to network hiccups and such.
# Based on the data source (the first character of the file), the output directory will either be 
#
#    MODIS_AQUA_L2_SST_OBPG
#    MODIS_TERRA_L2_SST_OBPG
#
# so the MODIS Level 2 Uncompressor (related to the MODIS Level 2 Combiner) can find them later.
#------------------------------------------------------------------------------------------------

import os
import sys
import pathlib
import time

from time import gmtime, strftime;

from delete_error_count                 import delete_error_count;
from handle_downloader_error_historical import handle_downloader_error_historical;
from log_this import log_this;
from perform_download_and_move          import perform_download_and_move;
from preparation_for_downloader         import preparation_for_downloader;
from write_final_log                    import write_final_log

import settings

#------------------------------------------------------------------------------------------------------------------------
def generic_level2_downloader_driver_via_fork_historical(i_pipe_writer,
                                                         i_batch_number,
                                                         i_sleep_time_in_between_files,
                                                         i_filelist_name,
                                                         i_many_lines,
                                                         i_separator_character,
                                                         i_processing_level,
                                                         i_processing_type,
                                                         i_top_level_output_directory,
                                                         i_perform_checksum_flag,
                                                         i_scratch_area,
                                                         i_today_date,
                                                         i_test_run_flag):

    # Output variable(s):

    o_download_driver_status = 0;  # A status of 0 means successful and 1 means failed.

    o_total_Bytes_in_files = 0;

    #g_routine_name = "generic_level2_downloader";
    g_routine_name = "generic_level2_downloader_driver_via_fork_historical";
    debug_module = "generic_level2_downloader_driver_via_fork_historical:";
    debug_mode = 0;

    if (os.getenv('CRAWLER_SEARCH_DEBUG_FLAG','') == 'true'):
        debug_mode = 1;

    if (debug_mode):
        print('i_pipe_writer',i_pipe_writer);
        print('i_batch_number',i_batch_number)
        print('i_sleep_time_in_between_files',i_sleep_time_in_between_files)
        print('i_filelist_name',i_filelist_name)
        print('i_many_lines',i_many_lines)
        print('i_separator_character',i_separator_character)
        print('i_processing_level',i_processing_level)
        print('i_processing_type',i_processing_type)
        print('i_top_level_output_directory',i_top_level_output_directory)
        print('i_perform_checksum_flag',i_perform_checksum_flag)
        print('i_scratch_area',i_scratch_area)
        print('i_today_date',i_today_date)
        print('i_test_run_flag',i_test_run_flag);

    # Some time related business.

    begin_processing_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime());
 
    log_this("INFO",g_routine_name,"BATCH_NUMBER " + str("{:03d}".format(i_batch_number)) + " BEGIN_PROCESSING_TIME " + begin_processing_time);

    # Do a sanity check to see if the generic downloader job manager (global g_gdjm) has been created yet.
#    global g_gdjm;

    if (settings.g_gdjm is None):
        print("The global object g_gdjm has not been created yet.  Cannot continue.");
        exit(1);

    # Time related variables used to keep track of how long things take.

    function_time_start = time.time();
    current_time = function_time_start;

    log_message   = ""; 

    # For every name in the list, we will attempt to download it.

    prepation_status = 1;
    download_status  = 1;

    checksum_value = "";
    checksum_status = 1;
    incomplete_filename = "";
    full_pathname_to_download = "";
    temporary_location_of_downloaded_file = "";
    final_location_of_downloaded_file = "";
    files_downloader_per_batch = 0;

    time_start_download    = 0;
    time_end_download      = 0; 
    time_elapsed_download  = 0;
    batch_elapsed_download  = 0;
    batch_start_download    = time.time();

    many_lines = i_many_lines; 
    if (debug_mode):
        print(debug_module + "len(many_lines) " + str(len(many_lines)));
        print(debug_module + "many_lines) ",many_lines);

    num_files_downloaded       = 0;
    num_success_downloads = 0
    num_failure_downloads = 0
    num_sst_sst4_files         = len(many_lines);
    file_count                 = 0;
    time_spent_in_downloading = 0.0;

    for one_line in many_lines:
        # A line from the download file look like:
        #
        #     https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2013144223500.L2_LAC_OC.bz2 b9ef4280290cecbb6d4a1c505f14fdcfd9d7b03b

        file_count = file_count + 1;

        if (debug_mode):
            print(debug_module + "type(one_line) [",type(one_line),"]");
            print(debug_module + "len(one_line) [",len(one_line),"]");
            print(debug_module + "one_line [",one_line,"]");

        # Prepare the downloader.

        (prepation_status,checksum_value,checksum_status,incomplete_filename,full_pathname_to_download,
         temporary_location_of_downloaded_file,final_location_of_downloaded_file) = preparation_for_downloader(one_line,
                                                                                                               i_separator_character,
                                                                                                               i_perform_checksum_flag,
                                                                                                               i_top_level_output_directory,
                                                                                                               i_processing_level,
                                                                                                               i_processing_type,
                                                                                                               i_filelist_name,
                                                                                                               i_scratch_area,
                                                                                                               i_today_date);

        if (debug_mode):
            print(debug_module + "prepation_status                      " + str(prepation_status));
            print(debug_module + "checksum_value                        " + checksum_value);
            print(debug_module + "checksum_status                       " + str(checksum_status));
            print(debug_module + "incomplete_filename                   " + incomplete_filename);
            print(debug_module + "full_pathname_to_download             " + full_pathname_to_download);
            print(debug_module + "temporary_location_of_downloaded_file " + temporary_location_of_downloaded_file);
            print(debug_module + "final_location_of_downloaded_file     " + final_location_of_downloaded_file);

        o_download_driver_status = prepation_status;

        # If cannot prepare the directories necessary for download, handle the error and skip to next file.
        # Remember that a value for prepation_status of 1 is bad.

        if (prepation_status == 1): 
            incomplete_filename = handle_downloader_error_historical(full_pathname_to_download,
                                                                     i_filelist_name,
                                                                     checksum_value);
        else:
            # Do the actual download one file at a time.

            time_start_download = time.time();
            log_this("INFO",g_routine_name,"DOWNLOADER_INITIATE  " + full_pathname_to_download + " " + temporary_location_of_downloaded_file);

            # If the developer wishes to ignore the downloading, we respect it.  This is to test the end to end of the downloader without actually doing the download.
            if (os.getenv('CRAWLER_SEARCH_DOWNLOADER_SKIP_DEVELOPER_FLAG','') == "true"):
                # Do nothing but to set some variables.
                download_status = 1;
                time_spent_in_downloading = 0.0;
            else:
                 (download_status, time_spent_in_downloading, final_location_of_downloaded_file) = perform_download_and_move(num_files_downloaded,
                                                                                          num_sst_sst4_files,
                                                                                          full_pathname_to_download,
                                                                                          temporary_location_of_downloaded_file,
                                                                                          final_location_of_downloaded_file,
                                                                                          time_spent_in_downloading,
                                                                                          i_perform_checksum_flag,
                                                                                          checksum_value,
                                                                                          i_test_run_flag);

            time_end_download = time.time();
            time_elapsed_download = time_end_download - time_start_download;
            log_this("INFO",g_routine_name,"DOWNLOADER_TERMINATE " + full_pathname_to_download + " " + temporary_location_of_downloaded_file + "  " + str('{:.2f}'.format(time_elapsed_download)));

            if (debug_mode):
                print(debug_module + "download_status           ", download_status);
                print(debug_module + "time_spent_in_downloading ", time_spent_in_downloading);

            # If CRAWLER_SEARCH_DEVELOPER_DOWNLOADER_DOWNLOAD_FAIL_FLAG is set by developer, we set download_status to signify the download had failed to trigger the writing of the incomplete file.
            if (os.getenv('CRAWLER_SEARCH_DEVELOPER_DOWNLOADER_DOWNLOAD_FAIL_FLAG','') == "true"):
                print(debug_module + "CRAWLER_SEARCH_DEVELOPER_DOWNLOADER_DOWNLOAD_FAIL_FLAG is true.  Setting download_status to 0");
                download_status = 0;

            # Report if any issues with the download.
            o_download_driver_status = download_status;
            if (download_status == 0):
                incomplete_filename = handle_downloader_error_historical(full_pathname_to_download,
                                                               i_filelist_name,
                                                               checksum_value);
                if (debug_mode):
                    print(debug_module + "incomplete_filename " + incomplete_filename);
#            else:
            # Regardless if the download was successful or not, we remove this registry since it is no good to anyone.
            # If we only remove the registry when the job is successful and not when it is not successful,
            # the registry will remain until the job monitor comes and clean it up, which is unnessary wait.
                        
            (o_register_status,o_location_of_registry_file) =  settings.g_gdjm.remove_this_job(one_line,
                                                                                               i_top_level_output_directory,
                                                                                               i_processing_level,
                                                                                               i_processing_type);

        size_of_sst_file_in_bytes  = 0;  # This size only change to the actual value if the download was a success and the file can be found.
        
        if (download_status == 1):
            if ((final_location_of_downloaded_file != "") and (not (os.path.isfile(final_location_of_downloaded_file)))):
                log_this("WARN",g_routine_name,"DOWNLOAD_SUCCESSFUL_BUT_FILE_NOT_FOUND " + final_location_of_downloaded_file);

        if ((download_status == 1) and (os.path.isfile(final_location_of_downloaded_file))):
            #
            # Get the size of this file.
            #
            size_of_sst_file_in_bytes  = os.stat(final_location_of_downloaded_file).st_size;

            print(f"{g_routine_name} - INFO: Processed: {one_line}")
            write_final_log(f"processed: {one_line}")
            log_this("INFO",g_routine_name,"DOWNLOAD_INFO: " + "FILE_DOWNLOADED " + final_location_of_downloaded_file + " FILE_SIZE " + str(size_of_sst_file_in_bytes) + " TIME_ELAPSED_DOWNLOAD " + str('{:.2f}'.format(time_elapsed_download)));
            log_this("INFO",g_routine_name,"DOWNLOAD_INFO: " + "BATCH_NUMBER "    + str("{:03d}".format(i_batch_number)) + " FILE_COUNT " + str("{:05d}".format(file_count)) + " FILE_DOWNLOADED " + final_location_of_downloaded_file + " DOWNLOAD_SUCCESS " + str('{:.2f}'.format(time_elapsed_download)) + " " + str(size_of_sst_file_in_bytes));
            o_total_Bytes_in_files += size_of_sst_file_in_bytes; 

            # If the file has been downloaded successfully, any error count file associated with it can be deleted.
            delete_error_count(full_pathname_to_download);

            # Write to pipe that the download was successful.
            # If the pipe is available, write the file name and some metadata about processing.

            if (i_pipe_writer is not None):
                log_this("INFO",g_routine_name,"WRITE_TO_PIPE: " + final_location_of_downloaded_file + " DOWNLOAD_SUCCESS " + str('{:.2f}'.format(time_elapsed_download)) + " " + str(size_of_sst_file_in_bytes));
                i_pipe_writer.write(final_location_of_downloaded_file + " DOWNLOAD_SUCCESS " + str('{:.2f}'.format(time_elapsed_download)) + " " + str(size_of_sst_file_in_bytes) + "\n");

            # Keep track of how many files we have downloaded for this batch.
            files_downloader_per_batch = files_downloader_per_batch + 1;
            num_files_downloaded       = num_files_downloaded       + 1;
            num_success_downloads += 1
        else:
            # Even if the download was a failure, we still have to write to the pipe since it needs to be read by the parent program.
            log_this("INFO",g_routine_name,"WRITE_TO_PIPE: " + final_location_of_downloaded_file + " DOWNLOAD_FAILURE " + str('{:.2f}'.format(time_elapsed_download)) + " " + str(size_of_sst_file_in_bytes));
            if (i_pipe_writer is not None):
                i_pipe_writer.write(final_location_of_downloaded_file + " DOWNLOAD_FAILURE " + str('{:.2f}'.format(time_elapsed_download)) + " " + str(size_of_sst_file_in_bytes) + "\n");
            num_failure_downloads += 1
            write_final_log(f"failed_download: {pathlib.Path(final_location_of_downloaded_file).name}")
        # end if ((download_status == 1) and (os.path.isfile(final_location_of_downloaded_file)))

        # Go to sleep if user request to sleep for a bit.
        if (i_sleep_time_in_between_files > 0):
            log_this("INFO",g_routine_name,"SLEEPING_IN_BETWEEN " + str(i_sleep_time_in_between_files) + " SECONDS");
            time.sleep(i_sleep_time_in_between_files);

    # end for one_line in many_lines

    function_time_end = time.time();
    elapsed_in_seconds = function_time_end - function_time_start;
    elapsed_in_minutes = "{:.2f}".format(elapsed_in_seconds/60.0);

    # ---------- Close up shop ----------

    # Print run statistics for this batch.

    batch_end_download    = time.time();
    batch_elapsed_download = batch_end_download - batch_start_download;

    if (num_files_downloaded > 0):
        average_downloader_per_file_in_str = str("{:.2f}".format(batch_elapsed_download/num_files_downloaded));
        log_this("INFO",g_routine_name,"FILES_STAT BATCH_NUMBER " + str("{:03d}".format(i_batch_number)) + " FILES_DOWNLOADED_PER_BATCH " + str(files_downloader_per_batch) + " FOR_BATCH_OF " + str(num_sst_sst4_files) + " BATCH_ELAPSED " + str('{:.2f}'.format(batch_elapsed_download)) + " BATCH_AVERAGE_DOWNLOAD_PER_FILE " + str(average_downloader_per_file_in_str));
    else:
        log_this("INFO",g_routine_name,"FILES_STAT BATCH_NUMBER " + str("{:03d}".format(i_batch_number)) + " FILES_DOWNLOADED_PER_BATCH " + str(files_downloader_per_batch) + " FOR_BATCH_OF " + str(num_sst_sst4_files) + " BATCH_ELAPSED " + str('{:.2f}'.format(batch_elapsed_download)));

    end_processing_time = strftime("%a %b %d %H:%M:%S %Y",gmtime());
    log_this("INFO",g_routine_name,"BATCH_NUMBER " + str("{:03d}".format(i_batch_number)) + " END_PROCESSING_TIME " + end_processing_time);

    return(num_files_downloaded,o_total_Bytes_in_files,num_success_downloads,num_failure_downloads);

if __name__ == "__main__":
    debug_module = "generic_level2_downloader_driver_via_fork_historical:";
    debug_mode   = 1;

    # Because we are running this as main, we have to explicitly call the init() function.
    settings.init();

    i_pipe_writer = None;
    i_batch_number = 1;
    i_sleep_time_in_between_files = 0;
    i_filelist_name = os.getenv('OBPG_RUNENV_RESOURCES_HOME','') + "/modis_aqua_filelist.txt.daily_2016_008_date_2016_01_08";
    i_many_lines = ['https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008000000.L2_LAC_OC.nc d2538d481de288cb0774e24b9458c59601b2cfe4'];
    i_separator_character = "SPACE";
    i_processing_level = "L2";
    i_processing_type = "MODIS_A";
    i_top_level_output_directory = "/data/dev/scratch/qchau/IO/data"
    i_perform_checksum_flag = "no"
    i_scratch_area = "/data/dev/scratch/qchau/scratch_temp2";
    i_today_date = "17_02_01_11_02_53"
    i_test_run_flag = None; 

    o_download_driver_status = generic_level2_downloader_driver_via_fork_historical(i_pipe_writer,
                                                         i_batch_number,
                                                         i_sleep_time_in_between_files,
                                                         i_filelist_name,
                                                         i_many_lines,
                                                         i_separator_character,
                                                         i_processing_level,
                                                         i_processing_type,
                                                         i_top_level_output_directory,
                                                         i_perform_checksum_flag,
                                                         i_scratch_area,
                                                         i_today_date,
                                                         i_test_run_flag);

    if (debug_mode):
        print('i_pipe_writer',i_pipe_writer);
        print('i_batch_number',i_batch_number)
        print('i_sleep_time_in_between_files',i_sleep_time_in_between_files)
        print('i_filelist_name',i_filelist_name)
        print('i_many_lines',i_many_lines)
        print('i_separator_character',i_separator_character)
        print('i_processing_level',i_processing_level)
        print('i_processing_type',i_processing_type)
        print('i_top_level_output_directory',i_top_level_output_directory)
        print('i_perform_checksum_flag',i_perform_checksum_flag)
        print('i_scratch_area',i_scratch_area)
        print('i_today_date',i_today_date)
        print('i_test_run_flag',i_test_run_flag);
        print('o_download_driver_status',o_download_driver_status);
    sys.exit(0);
