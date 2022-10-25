#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# @version $Id$
#****************************************************************************
# This is a function to perform the downloading of files given a list of file names from OBPG.
#
# The URI is in the form of:
#
#     https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2013048142500.L2_LAC_SST.nc
#
# If the URI is just a file name, this script will preceed the URI with the string "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to pass along
# to the generic_level2_downloader_driver_via_fork_historical() to perform the download.
#
# If the file contains comma or space separated values, this script can support it too.
# If the checksum is provided, it should be separated by a space or comma.  The checksum type is assumed to be SHA-1.
#
#------------------------------------------------------------------------------------------------

import datetime
import math
import os
import random
import re
import sys
import time

from   multiprocessing import Process
from   time import gmtime, strftime

# Define user-defined classes and subroutines.

import settings;

from generic_downloader_job_manager import *;
from generic_level2_downloader_driver_via_fork_historical import generic_level2_downloader_driver_via_fork_historical;
from get_local_time                 import get_local_pdt_time;
from log_this import log_this;
from move_to_processed_download_directory import move_to_processed_download_directory;
from raise_sigevent_wrapper     import raise_sigevent_wrapper;

#------------------------------------------------------------------------------------------------------------------------
def generic_level2_downloader_driver_historical_using_process(i_filelist_name,
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
                                                i_test_run_flag):

    debug_module = "generic_level2_downloader_driver_historical_using_process:";
    debug_mode   = 0;

    if (os.getenv('CRAWLER_SEARCH_DEBUG_FLAG','') == "true"):
        debug_mode   = 1;

    NUM_FILES_TO_PROCESS = 200000;  # Default number of files to process.  Will reset to smaller number if input is smaller.

    # Create a global object so we can manage the download jobs.
    settings.init();

    # Output variable(s):

    o_download_driver_status = 0;  # A status of 0 means successful and 1 means failed.

    debug_module = "generic_level2_downloader_driver_historical_using_process:";
    g_routine_name   = "generic_level2_downloader_driver_historical_using_process";

    if (debug_mode):
        print(debug_module + "i_filelist_name",i_filelist_name);
        print(debug_module + "i_separator_character",i_separator_character);
        print(debug_module + "i_processing_level",i_processing_level);
        print(debug_module + "i_processing_type",i_processing_type);
        print(debug_module + "i_top_level_output_directory",i_top_level_output_directory);
        print(debug_module + "i_num_files_to_download",i_num_files_to_download);
        print(debug_module + "i_sleep_time_in_between_files",i_sleep_time_in_between_files);
        print(debug_module + "i_move_filelist_file_when_done",i_move_filelist_file_when_done);
        print(debug_module + "i_perform_checksum_flag",i_perform_checksum_flag);
        print(debug_module + "i_today_date",i_today_date);
        print(debug_module + "i_job_full_name",i_job_full_name);
        print(debug_module + "i_test_run_flag",i_test_run_flag);

    # Some time related business.

    localtime = get_local_pdt_time();
    begin_processing_time = strftime("%a %b %d %H:%M:%S %Y",localtime); 
    log_this("INFO",g_routine_name,"BEGIN_PROCESSING_TIME " + begin_processing_time);

    # Some variables related to sigevent.

    sigevent_url = os.getenv('GHRSST_SIGEVENT_URL','');
    sigevent_source = "GHRSST-PROCESSING";
    if (sigevent_url == ''):
        print("You must defined the sigevent URL: i.e. setenv GHRSST_SIGEVENT_URL http://test.jpl.nasa.gov:8100"); 
        sys.exit(0);

    # Time related variables used to keep track of how long things take.

    program_time_start = time.time() ;
    current_time = program_time_start;

    log_message   = ""; 

    # Crawl for a list of files in input directory.

    time_start_loading = time.time();

    # Do a sanity check on the existence of the input file.

    if (not os.path.isfile(i_filelist_name)):
        log_this("WARN",g_routine_name,"NO_FILES_FOUND " + i_filelist_name);
        sys.exit(0);

    #  Read the entire file into list_of_lines_from_download_file.
    list_of_lines_from_download_file = [];
    log_this("INFO",g_routine_name,"FILE_READ_DOWNLOAD_INPUT_FILE " + i_filelist_name);
    try:
        f = open(i_filelist_name);
    except IOError:
        print(debug_module + "ERROR: Cannot open file " + i_filelist_name);
        o_files_download_status = 0;
        return(o_files_download_status);
    else:
        with f:
            list_of_lines_from_download_file = f.readlines();


    # Because it is possible for the file to contain blanks lines, we will attempt clear these.

    list_of_files_to_download = [];
    for one_line in list_of_lines_from_download_file:
        if (one_line.strip().rstrip("\n") != ""):
            # By default, we replace any links "http://" with "https://" because OBPG uses https for as protocol.
            one_line = one_line.replace("http://","https://");
            list_of_files_to_download.append(one_line);

    if (debug_mode):
       print("type(list_of_files_to_download)",type(list_of_files_to_download));
       print("len(list_of_files_to_download)",len(list_of_files_to_download));
       print("list_of_files_to_download",list_of_files_to_download)

    time_end_loading = time.time();
    time_end_in_loading = time_end_loading - time_start_loading; 

    scratch_area = os.getenv('SCRATCH_AREA','');

    # Move the file list file to a different directory if the user requests it.
    # During normal operation, this will be done.  During development, the file list file may be kept around for a shortwhile.
    # Since these file are named as modis_aqua_filelist.txt.0001 with the last 4 digits incrementing, it is likely that no useful
    # information can be retrieved from the name since they do not tell when these files are created.  The name are used over
    # and over again.  We move them to a different directory to signify that we have processed all the names and allow this same
    # can be used by the modis-rdac handler again.
    #
    # Note: The reason why we are moving the file here is because if another process starts up while this process is running, 
    #       a potential conflict can take place where the same file is being processed by 2 different processes.  To prevent
    #       such problem, we move the file as soon as the file gets loaded in memory.


    log_this("INFO",g_routine_name,"MOVE_FILELIST_FILE_WHEN_DONE " + i_move_filelist_file_when_done);
    if (i_move_filelist_file_when_done == "yes"):
        print("move_to_processed_download_directory",i_processing_type,i_filelist_name,scratch_area,i_today_date);
        status_move = move_to_processed_download_directory(i_processing_type,i_filelist_name,scratch_area,i_today_date);

    num_sst_sst4_files = len(list_of_files_to_download);


    # Return if there's nothing to do.
    if (num_sst_sst4_files == 0):
        localtime = get_local_pdt_time();
        end_processing_time = strftime("%a %b %d %H:%M:%S %Y",localtime);#datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Y");
        log_this("INFO",g_routine_name,"NO_FILES_FOUND " + i_filelist_name);
        log_this("INFO",g_routine_name,"BEGIN_PROCESSING_TIME " + begin_processing_time);
        log_this("INFO",g_routine_name,"END_PROCESSING_TIME   " + end_processing_time);
        sys.exit(0);

    log_this("INFO",g_routine_name,"BEGIN_DOWNLOADING " + str(num_sst_sst4_files) + " NUM_FILES TO " + i_top_level_output_directory); 

    # Everything is OK, we can proceed with the downloading task.

    num_files_read            = 0;
    num_files_downloaded      = 0;
    time_spent_in_downloading = 0; 
    total_Bytes_in_files      = 0; 
    total_Bytes_downloaded    = 0; 

    # Reset the number of files to process if input is smaller.

    if (i_num_files_to_download < NUM_FILES_TO_PROCESS):
        NUM_FILES_TO_PROCESS = i_num_files_to_download;

    # For every name in the list, we will attempt to download it.

    prepation_status = 1;
    download_status  = 1;

    index_to_sst_sst4_list = 0;
    checksum_value = "";
    checksum_status = 1;
    incomplete_filename = "";
    full_pathname_to_download = "";
    temporary_location_of_downloaded_file = "";
    final_location_of_downloaded_file = "";

    time_start_download    = 0;
    time_end_download      = 0; 
    time_elapsed_download  = 0;
    time_total_download  = 0.0;    # Get the total time it took to download each file (even though they were downloaded in parallel).
    num_subprocesses_started = 0; 

    MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START = 2;

    # If user provided a value, we use it to wait for in between sub process start.
    if (os.getenv('OBPG_RUNENV_MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START','') != ''):
        MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START = int(os.getenv('OBPG_RUNENV_MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START',''));

    log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO os.getenv(OBPG_RUNENV_MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START) [" + os.getenv('OBPG_RUNENV_MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START','') +  "]");
    log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START " + str(MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START));

    # By default, use the parallel processing mode unless the user decided against it.
    use_parallel_processing_flag = 1;
    DUMMY_PROCESS_ID = 99999;  # Set this child process id to something we don't normally expect to represent running the parent process.
    child_pid        = DUMMY_PROCESS_ID;  # Set this child process id to something we don't normally expect to represent running the parent process.
    if (os.getenv('OBPG_RUNENV_USE_PARALLEL_PROCESSING','') == "false"):
        use_parallel_processing_flag = 0;

    log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO use_parallel_processing_flag " + str(use_parallel_processing_flag));

    use_pipe_flag = 1; # Developer's flag to use pipe to communicate between the child processes and the parent.
    if (os.getenv('OBPG_RUNENV_USE_PIPE_FLAG','') == "false"):
        use_pipe_flag = 0;

    # Because the number of jobs can exceed many characters, we allow a larger buffer for in our ledger.
    DEFAULT_MAX_PIPE_BUFFER_SIZE = 102400;
    if (os.getenv('OBPG_RUNENV_MAX_PIPE_BUFFER_SIZE','') != ''):
        DEFAULT_MAX_PIPE_BUFFER_SIZE = int(os.getenv('OBPG_RUNENV_MAX_PIPE_BUFFER_SIZE',''));
    master_pipe = None;       # The master_pipe will be used by the parent process to read what the child write out processing statistics.
    child_pipe  = None;       # The child_pipe will be used by child processes to write out processing statistics.
    if (use_pipe_flag):
        master_pipe,child_pipe = os.pipe();
        master_pipe,child_pipe = os.fdopen(master_pipe,'r',0), os.fdopen(child_pipe,'w',0)
        log_this("INFO",g_routine_name,"MASTER_PIPE_CREATE_SUCCESS");

    # We will use a child ledger to register jobs when they started to run and the master ledger to check for jobs completion.
    # This is our way to check if a job is taking too long to alert the operator.
    

    master_ledger = None;
    child_ledger = None;
    if (use_pipe_flag):
        master_ledger,child_ledger = os.pipe();
        master_ledger,child_ledger = os.fdopen(master_ledger,'r',DEFAULT_MAX_PIPE_BUFFER_SIZE), os.fdopen(child_ledger,'w',DEFAULT_MAX_PIPE_BUFFER_SIZE)

    if (debug_mode) :
        print(debug_module + "master_ledger[",master_ledger,"]");
        print(debug_module +"child_ledger [",child_ledger,"]");

    MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN = 1;
    if (os.getenv('OBPG_RUNENV_MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN','') != ''):
        MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN = int(os.getenv('OBPG_RUNENV_MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN',''));

    log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN " + str(MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN));

    # Calculate the number of files to download per batch.  This is dependant on the value of MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN and the number of files to download.

    batch_size = 0;   
    if (NUM_FILES_TO_PROCESS > num_sst_sst4_files):
        batch_size = int(math.ceil(num_sst_sst4_files/MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN));
    else:
        batch_size = int(math.ceil(NUM_FILES_TO_PROCESS/MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN));
        log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO batch_size:math.ceil:batch_size " + str(batch_size));
        

    # It is possible for the batch_size to be less than or equal to zero.  We make a slight tweak to get at least 1.
    if (batch_size <= 0):
        batch_size = 1;
        log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO batch_size:after_tweak:batch_size " + str(batch_size));

    log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO num_sst_sst4_files " + str(num_sst_sst4_files));
    log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO NUM_FILES_TO_PROCESS " + str(NUM_FILES_TO_PROCESS));

    log_this("INFO",g_routine_name,"MULTI_PROCESS_INFO BATCH_SIZE " + str(batch_size));

    num_filenames_dispatched_for_download_total = 0;
    num_batches_created = 0;
    num_processes_created   = 0;
    names_printed_for_test = 0;

    all_of_lines_to_download = [];

    num_calls_to_get_batch_of_lines = 0;
    loop_index = 0;

    process_runners_list = [];
#    debug_mode = 1;
    while ((index_to_sst_sst4_list < num_sst_sst4_files) and (num_files_downloaded < NUM_FILES_TO_PROCESS)):
        num_filenames_dispatched_per_batch  = 0;
        batch_of_lines_to_download = [];

        num_calls_to_get_batch_of_lines += 1;
        millis = int(round(time.time() * 1000));
        if (debug_mode):
            log_this("INFO",g_routine_name,"PRE:CALL:GET_BATCH_OF_LINES " + "loop_index" + " " + str(loop_index) +  " index_to_sst_sst4_list " + str(index_to_sst_sst4_list) + " MILLISECONDS " + str(millis) + " num_calls_to_get_batch_of_lines " + str(num_calls_to_get_batch_of_lines));

        (batch_of_lines_to_download,index_to_sst_sst4_list,num_files_downloaded,num_filenames_dispatched_per_batch) = get_batch_of_lines(list_of_files_to_download,index_to_sst_sst4_list,batch_size,num_files_downloaded,NUM_FILES_TO_PROCESS);

        if (debug_mode):
            log_this("INFO",g_routine_name,"POST:CALL:GET_BATCH_OF_LINES " + "loop_index" + " " + str(loop_index) + " index_to_sst_sst4_list " + str(index_to_sst_sst4_list) + " MILLISECONDS " + str(millis) + " num_calls_to_get_batch_of_lines " + str(num_calls_to_get_batch_of_lines));

        if (debug_mode):
            print(debug_module + "type(batch_of_lines_to_download)",type(batch_of_lines_to_download));
            print(debug_module + "len(batch_of_lines_to_download)",len(batch_of_lines_to_download));
            print(debug_module + "batch_of_lines_to_download",batch_of_lines_to_download);
            print(debug_module + "index_to_sst_sst4_list",index_to_sst_sst4_list);
            print(debug_module + "num_files_downloaded",num_files_downloaded);
            print(debug_module + "num_filenames_dispatched_per_batch",num_filenames_dispatched_per_batch);

        # Do a sanity check if cannot get any lines to download from the list_of_files_to_download.  Something is wrong for that to happen.
        if (len(batch_of_lines_to_download) <= 0):
            print(debug_module + "ERROR: Something went wrong with getting a batch of lines to download from list_of_files_to_download.  Program exiting.");
            sys.exit(0);

        # Keep track of how many batches created and how many names collected so far.
        num_batches_created = num_batches_created + 1;
        num_filenames_dispatched_for_download_total = num_filenames_dispatched_for_download_total + num_filenames_dispatched_per_batch;

        # If running a test by developer, we will print the list of names per batch, and skip the actual download of files.

        if (os.getenv('OBPG_RUNENV_PRINT_BATCHES_CONTENT_ONLY_TEST','') == "true"):
            child_ledger  = None;
            master_ledger = None;
            all_of_lines_to_download = print_batch_of_jobs_content(child_ledger,
                                                                   master_ledger,
                                                                   num_filenames_dispatched_per_batch,
                                                                   all_of_lines_to_download,
                                                                   batch_of_lines_to_download,
                                                                   num_batches_created,
                                                                   debug_module);
            continue; # Move on to next name without doing downloading.   
        else:
            # Write to ledger to register the jobs.  Note that this should be done before the sub processes creation so we can register immediately instead of letting
            # the sub process register in case there are issue with the fork() function.
            if (debug_mode):
                log_this("INFO",g_routine_name,"CALL:REGISTER_JOBS_IN_BATCH:child_pid " + str(child_pid) + " " + "len(batch_of_lines_to_download) " + str(len(batch_of_lines_to_download)) + " index_to_sst_sst4_list " + str(index_to_sst_sst4_list) + " num_files_downloaded " + str(num_files_downloaded) + " num_filenames_dispatched_per_batch " + str(num_filenames_dispatched_per_batch) + " num_sst_sst4_files " + str(num_sst_sst4_files) + " NUM_FILES_TO_PROCESS " + str(NUM_FILES_TO_PROCESS) + " num_batches_created " + str(num_batches_created) + " num_filenames_dispatched_for_download_total " + str(num_filenames_dispatched_for_download_total));
            if (use_pipe_flag):
                register_jobs_in_batch(child_ledger,
                                       batch_of_lines_to_download,
                                       i_top_level_output_directory,
                                       i_processing_level,
                                       i_processing_type,
                                       num_filenames_dispatched_per_batch,
                                       num_batches_created);
            else:
                 register_jobs_in_batch_via_file(scratch_area,
                                       batch_of_lines_to_download,
                                       i_top_level_output_directory,
                                       i_processing_level,
                                       i_processing_type,
                                       num_filenames_dispatched_per_batch,
                                       num_batches_created);
            

            # After the above call, each job is now in a ledger.  The master_ledger can then be read and the jobs monitored.
        # end if (os.getenv('OBPG_RUNENV_PRINT_BATCHES_CONTENT_ONLY_TEST','') == "true")

        # A line from the download file look like:
        #
        #     http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2013144223500.L2_LAC_OC.bz2 b9ef4280290cecbb6d4a1c505f14fdcfd9d7b03b

        working_on_this_batch = "BATCH_NUMBER_" + str('{:03d}'.format(num_batches_created)) + " NUM_FILENAMES_DISPATCHED_PER_BATCH " + str('{:04d}'.format(num_filenames_dispatched_per_batch));

        # To do parallel processing, we add the Process() call to allow the code below to run in a separate process.


        if (use_parallel_processing_flag):
            log_this("INFO",g_routine_name,"SLEEP MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START " + str(MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START));
            time.sleep(MAX_NUM_SECONDS_IN_BETWEEN_SUBPROCESS_START);
            num_processes_created = num_processes_created + 1;
            child_pid = 77777;
            # This next function is executed when the multiprocessing.Process() is called.  It will run in parallel for each fork() execution.
            # Note that this is the main job the child process is running.  After coming back, the sequence of execution is to close any writer pipes and exit.

            if (debug_mode):
                print(debug_module + "index_to_sst_sst4_list",index_to_sst_sst4_list);
                print(debug_module + "num_sst_sst4_files",num_sst_sst4_files);
                print(debug_module + "num_files_downloaded",num_files_downloaded);
                print(debug_module + "NUM_FILES_TO_PROCESS",NUM_FILES_TO_PROCESS);

            log_this("INFO",g_routine_name,"MULTI_PROCESS_PROCESSS:ACTIVATE:GENERIC_LEVEL2_DOWNLOADER_DRIVER_VIA_FORK_HISTORICAL:child_pid " + str(child_pid));
            process_runner_obj = Process(name='generic_level2_downloader_driver_via_fork_historical' + '_' + str(loop_index),target=generic_level2_downloader_driver_via_fork_historical,args=(child_pipe,
                                                                                                                                                                                               num_batches_created,
                                                                                                                                                                                               i_sleep_time_in_between_files,
                                                                                                                                                                                               i_filelist_name,
                                                                                                                                                                                               batch_of_lines_to_download,
                                                                                                                                                                                               i_separator_character,
                                                                                                                                                                                               i_processing_level,
                                                                                                                                                                                               i_processing_type,
                                                                                                                                                                                               i_top_level_output_directory,
                                                                                                                                                                                               i_perform_checksum_flag,
                                                                                                                                                                                               scratch_area,
                                                                                                                                                                                               i_today_date,
                                                                                                                                                                                               i_test_run_flag));
            # Now that we have instantiated a Process object, we can start it, and add the Process object to the list process_runners_list so we can check for
            # their completions outside of the while loop.

            log_this("INFO",g_routine_name,"MULTI_PROCESS_PROCESS_START  NUM_BATCHES_CREATED " + str(num_batches_created) + " BATCH_SIZE " + str(len(batch_of_lines_to_download)));
            process_runner_obj.start();
            process_runners_list.append(process_runner_obj);
            log_this("INFO",g_routine_name,"MULTI_PROCESS_PROCESS_APPEND NUM_BATCHES_CREATED " + str(num_batches_created) + " BATCH_SIZE " + str(len(batch_of_lines_to_download)) + " PROCESS_RUNNERS_LIST_LENGTH " + str(len((process_runners_list))));

        if (child_pid is None):
            log_this("ERROR",g_routine_name,"Cannot fork on processing " + working_on_this_batch);
            sys.exit(0);
        elif ((child_pid == 0) or (child_pid == DUMMY_PROCESS_ID)):
            # Only increment the num_subprocesses_started if we running as a sub process.
            if (debug_mode):
                log_this("INFO",g_routine_name,"MULTI_PROCESS_IN_CHECK CHILD_PID " + str(child_pid));

            if (child_pid == 0):
                num_subprocesses_started = num_subprocesses_started + 1;
                log_this("INFO",g_routine_name,"MULTI_PROCESS_IN_CHILD_PROCESS_RUNNING_BEGIN " + working_on_this_batch);
                log_this("INFO",g_routine_name,"OS_FORK_STARTED CHILD_PID " + str(child_pid) + " NUM_PROCESS_STARTED " + str(num_subprocesses_started));
#            else:
#                log_this("INFO",g_routine_name,"MULTI_PROCESS_IN_PARENT_PROCESS_RUNNING_BEGIN " + working_on_this_batch);

            # This next function is executed if running in sequential.

            log_this("INFO",g_routine_name,"SINGLE_PROCESS_IN_PARENT_PROCESS:ACTIVATE:GENERIC_LEVEL2_DOWNLOADER_DRIVER_VIA_FORK_HISTORICAL:child_pid " + str(child_pid));
            (num_files_read,total_Bytes_in_files) = generic_level2_downloader_driver_via_fork_historical(child_pipe,
                                                                 num_batches_created,
                                                                 i_sleep_time_in_between_files,
                                                                 i_filelist_name,
                                                                 batch_of_lines_to_download,
                                                                 i_separator_character,
                                                                 i_processing_level,
                                                                 i_processing_type,
                                                                 i_top_level_output_directory,
                                                                 i_perform_checksum_flag,
                                                                 scratch_area,
                                                                 i_today_date,
                                                                 i_test_run_flag);

            # If running this as a child sub process we need to exit to signify that we are done with the downloader job.
            if (child_pid == 0): 
                log_this("INFO",g_routine_name,"MULTI_PROCESS_IN_CHILD_PROCESS_RUNNING_END " + working_on_this_batch + " CHILD_PIPE_CLOSE");
                child_pipe.close();
                sys.exit(0); # Because this part of the code is run in a subprocess, we need to exit here to signify that we are done with the downloader job.
            else:
                # DOING SEQUENTIAL:PARENT_PROCESS: Do some house keeping to keep the indices in check.
                log_this("INFO",g_routine_name,"SINGLE_PROCESS_IN_PARENT_PROCESS_RUNNING_END " + working_on_this_batch);

                log_this("INFO",g_routine_name,"SINGLE_PROCESS_IN_PARENT_PROCESS_RUNNING_END_PRE_HOUSE_KEEPING len(all_of_lines_to_download) " + str(len(all_of_lines_to_download)) + " num_filenames_dispatched_per_batch " + str(num_filenames_dispatched_per_batch) + " num_subprocesses_started " + str(num_subprocesses_started));
                if (master_ledger is not None):
                    (child_pid,num_subprocesses_started,all_of_lines_to_download) = parent_house_keeping_of_ledger(master_ledger,
                                                                                                                   num_filenames_dispatched_per_batch,
                                                                                                                   all_of_lines_to_download,
                                                                                                                   num_subprocesses_started,
                                                                                                                   working_on_this_batch,
                                                                                                               DUMMY_PROCESS_ID);
                else:
                    (child_pid,num_subprocesses_started,all_of_lines_to_download) = parent_house_keeping_of_ledger_via_file(scratch_area,
                                                                                                                           i_processing_level,
                                                                                                                           i_processing_type,
                                                                                                                           num_filenames_dispatched_per_batch,
                                                                                                                           all_of_lines_to_download,
                                                                                                                           num_subprocesses_started,
                                                                                                                           working_on_this_batch,
                                                                                                                           DUMMY_PROCESS_ID);

                log_this("INFO",g_routine_name,"SINGLE_PROCESS_IN_PARENT_PROCESS_RUNNING_END_POST_HOUSE_KEEPING len(all_of_lines_to_download) " + str(len(all_of_lines_to_download)) + " num_filenames_dispatched_per_batch " + str(num_filenames_dispatched_per_batch) + " num_subprocesses_started " + str(num_subprocesses_started));
        else:
            # DOING PARALLEL:PARENT_PROCESS: Do some house keeping to keep the indices in check.
            if (os.getenv('OBPG_RUNENV_PRINT_BATCHES_CONTENT_ONLY_TEST','') == "true"):
                # If print the batches content only, there's nothing to do and no ledger to check.
                log_this("INFO",g_routine_name,"OBPG_RUNENV_PRINT_BATCHES_CONTENT_ONLY_TEST TRUE NO_HOUSE_KEEPING_REQUIRED");
                pass;
            else:
                if (master_ledger is not None):
                    (child_pid,num_subprocesses_started,all_of_lines_to_download) = parent_house_keeping_of_ledger(master_ledger,
                                                                                                                   num_filenames_dispatched_per_batch,
                                                                                                                   all_of_lines_to_download,
                                                                                                                   num_subprocesses_started,
                                                                                                                   working_on_this_batch,
                                                                                                                   DUMMY_PROCESS_ID);
                else:
                    (child_pid,num_subprocesses_started,all_of_lines_to_download) = parent_house_keeping_of_ledger_via_file(scratch_area, 
                                                                                                                           i_processing_level,
                                                                                                                           i_processing_type,
                                                                                                                           num_filenames_dispatched_per_batch,
                                                                                                                           all_of_lines_to_download,
                                                                                                                           num_subprocesses_started,
                                                                                                                           working_on_this_batch,
                                                                                                                           DUMMY_PROCESS_ID);
    
                log_this("INFO",g_routine_name,"MULTI_PROCESS_IN_PARENT_PROCESS CHILD_PID " + str(child_pid) + " NUM_SUBPROCESSES_STARTED " + str(num_subprocesses_started) + " NUM_FILENAMES_DISPATCHED_PER_BATCH " + str(num_filenames_dispatched_per_batch) + " len(all_of_lines_to_download)" + str(len(all_of_lines_to_download)));
            loop_index += 1;
            log_this("INFO",g_routine_name,"MULTI_PROCESS_IN_PARENT_PROCESS CHILD_PID " + str(child_pid) + " INCREMENTED_LOOP_INDEX " + str(loop_index));

    # end while ((index_to_sst_sst4_list < num_sst_sst4_files) and (num_files_downloaded < NUM_FILES_TO_PROCESS))


    # If we got to here, there may be jobs running in parallel, we now do a waiting game for all of them to finished.
    # The printing of the process_check may occur later in the log file but the time stamp may be the same time as the sub processes because
    # they are happening at the same time.
    # Note:  This next segment of the code is optional.  It only check to see if the processes started are alive or not.  The actual
    #        job check is a little further down by the Generic Downloader Job Monitor's object's function monitor_job_completion() where
    #        a sigevent will be raised if all the jobs have not completed within an alloted time. 

    skip_process_check_flag = True;

    at_least_one_process_is_still_running = True;
    debug_mode = 0;
    process_module = "process_check";

    check_time_start = time.time() ;

    while (not skip_process_check_flag and at_least_one_process_is_still_running):
        now_is = time.strftime("%c");
        if (debug_mode):
            print(process_module + 'Current time %s' % now_is);
            print(process_module + 'Sleeping for 10 seconds to give process time to come back');
            print(process_module + 'time.asctime()',time.asctime());

        # Check through all the processes in the list to make see if they are still alive.
        at_least_one_process_is_still_running = False;  # Start out with False.  Will be set to True if at least one process's is_alive() is true.
        process_runner_index = 0;

        for process_runner_obj in process_runners_list:
            process_is_still_running = process_runner_obj.is_alive();
            if (debug_mode):
                print(process_module + 'process_runner_index',process_runner_index,'process_runner_obj',process_runner_obj,'process_is_still_running',process_is_still_running)
            if (process_is_still_running):
                at_least_one_process_is_still_running = True;
            process_runner_index += 1;
        # end for process_runner_obj in process_runners_list

        if (debug_mode):
            print(process_module + 'at_least_one_process_is_still_running',at_least_one_process_is_still_running);
            log_this("INFO",process_module,'len(process_runners_list) ' + str(len(process_runners_list)) + ' at_least_one_process_is_still_running ' + str(at_least_one_process_is_still_running));

        if ((len(process_runners_list) > 0) and at_least_one_process_is_still_running):
            log_this("INFO",process_module,'len(process_runners_list) ' + str(len(process_runners_list)) + ' is more than zero and at_least_one_process_is_still_running, sleeping for 10 seconds');
            time.sleep(10);
    # end while (at_least_one_process_is_still_running)

    # If only doing a print test, we set some environment variable to some very small number for the function monitor_job_completion to exit early.
    check_time_stop = time.time() ;
    check_time_duration = check_time_stop - check_time_start;
    log_this("INFO",process_module,' len(process_runners_list) ' + str(len(process_runners_list)) + ' check_time_duration ' + str("{:.2f}".format(check_time_duration)));

    if (os.getenv('OBPG_RUNENV_PRINT_BATCHES_CONTENT_ONLY_TEST','') == 'true'):
        print(debug_module + "Environment OBPG_RUNENV_PRINT_BATCHES_CONTENT_ONLY_TEST is true.  Exiting");
        MODIS_LEVEL2_CONST_MAX_AGE_BEFORE_CONSIDERED_STALE = 0;
        MODIS_LEVEL2_CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS = 1;
        exit(0);
    
    # Now that we are done with registering the jobs in the ledger, we can close them.
    if (child_ledger is not None):
        child_ledger.close();
    if (master_ledger is not None):
        master_ledger.close();
    else:
        delete_file_for_interprocess_communication();

    # At this point, all the jobs should have started.  We monitor for their completion.
    if (debug_mode):
        print(debug_module + "num_filenames_dispatched_for_download_total",num_filenames_dispatched_for_download_total);
        print(debug_module + "i_top_level_output_directory",i_top_level_output_directory);
        print(debug_module + "i_processing_type",i_processing_type);
        print(debug_module + "all_of_lines_to_download",all_of_lines_to_download);

    (o_all_jobs_are_completed,o_num_incompleted_jobs,o_hidden_download_directory,o_total_seconds_waited) = settings.g_gdjm.monitor_job_completion(num_filenames_dispatched_for_download_total,
                                                                                                                                                  i_top_level_output_directory,
                                                                                                                                                  i_processing_level,
                                                                                                                                                  i_processing_type,
                                                                                                                                                  all_of_lines_to_download);

    if (debug_mode):
        print(debug_module + "o_all_jobs_are_completed " + str(o_all_jobs_are_completed) + " o_num_incompleted_jobs " + str(o_num_incompleted_jobs) + " o_hidden_download_directory " + str(o_hidden_download_directory) + " o_total_seconds_waited " + str(o_total_seconds_waited));


    # Let the operator know if some jobs took too long to complete.
    if (not o_all_jobs_are_completed):
        log_this("ERROR",g_routine_name,"MONITOR_JOB_COMPLETION:FAILED_SOME_DOWNLOAD_JOBS_MAY_NOT_COMPLETE_YET NUM_INCOMPLETE_JOBS " + str(o_num_incompleted_jobs) + " OUT_OF " + str(num_filenames_dispatched_for_download_total) + " o_total_seconds_waited " + str(o_total_seconds_waited));
        # Some variables related to sigevent.

        sigevent_type = "ERROR";
        sigevent_category = "GENERATE";
        sigevent_description = "MONITOR_JOB_COMPLETION:FAILED_SOME_DOWNLOAD_JOBS_MAY_NOT_COMPLETE_YET NUM_INCOMPLETE_JOBS " + str(o_num_incompleted_jobs) + " OUT_OF " + str(num_filenames_dispatched_for_download_total) + " o_total_seconds_waited " + str(o_total_seconds_waited); 
        sigevent_data = "Please inspect directory " + o_hidden_download_directory + " for stale file associated with a download.";
        sigevent_debug_flag = None;
        print(debug_module + "ERROR:" + sigevent_description);
        raise_sigevent_wrapper(sigevent_type,
                               sigevent_category,
                               sigevent_description,
                               sigevent_data,
                               sigevent_debug_flag);

    # Close the child pipe if no sub process started (i.e. not running in parallel) and it is still open.
    if ((num_subprocesses_started == 0) or not use_parallel_processing_flag):
        # Only close the child_pipe if it is define.
        if (child_pipe is not None):
            if (debug_mode):
                log_this("INFO",g_routine_name,"CHILD_PIPE_CLOSE");
            child_pipe.close();

    # Now that all the sub processes have started (if running in parallel), we collect the processing info written to the child_pipe by each processes.

    if ((num_subprocesses_started > 0) or (num_filenames_dispatched_for_download_total > 0)):
        log_this("INFO",g_routine_name,"MULTI_PROCESS_IN_PARENT_PROCESS FINAL_CHECK NUM_SUBPROCESSES_STARTED " + str(num_subprocesses_started) + " NUM_FILENAMES_DISPATCHED_FOR_DOWNLOAD_TOTAL " + str(num_filenames_dispatched_for_download_total));
        time_per_batch = 0;
        if (master_pipe is not None):
            (num_files_read,time_per_batch,total_Bytes_in_files) = inspect_running_subprocesses_for_completion(master_pipe,
                                                                                                               num_filenames_dispatched_for_download_total);
            time_total_download = time_total_download + time_per_batch;

    # Close the master_pipe since we are done.
    if (master_pipe is not None):
        if (debug_mode):
            log_this("INFO",g_routine_name,"MASTER_PIPE_CLOSE");
        master_pipe.close();

    # If successfully downloaded all files, remove the job file.

    if ((i_job_full_name is not None) and os.path.isfile(i_job_full_name)):
        log_this("INFO",g_routine_name,"REMOVE_JOB_FILE " + i_job_full_name); 
        os.remove(i_job_full_name);

    program_time_end = time.time();
    elapsed_in_seconds = program_time_end - program_time_start;
    elapsed_in_minutes = "{:.2f}".format(elapsed_in_seconds/60.0);

    # ---------- Close up shop ----------

    #
    # Variables related to disk space calculation.
    #

    Kilobyte_to_Byte_conversion_factor     = 1024;       # Kilobyte_const in Bytes
    Megabyte_to_Byte_conversion_factor     = 1048576;    # Megabyte_const in Bytes
    Gigabyte_to_Byte_conversion_factor     = 1073741824; # in Bytes
    Gigabyte_to_Megabyte_conversion_factor = 1024;       # in Megabyte

    total_Megabytes_in_files = total_Bytes_in_files / Megabyte_to_Byte_conversion_factor;
    total_Gigabytes_in_files = total_Bytes_in_files / Gigabyte_to_Byte_conversion_factor;

    # Print run statistics.

    log_this("INFO",g_routine_name,"TIME_STAT Seconds_Spent_In_Downloading      " + str("{:.2f}".format(elapsed_in_seconds)));
    log_this("INFO",g_routine_name,"FILES_STAT Number_of_files_read             " + str(num_files_read) + " OUT_OF " + str(num_sst_sst4_files));
    log_this("INFO",g_routine_name,"FILES_STAT total_Bytes_in_files             " + str(total_Bytes_in_files));
    log_this("INFO",g_routine_name,"FILES_STAT total_Megabytes_in_files         " + str(total_Megabytes_in_files));
    log_this("INFO",g_routine_name,"FILES_STAT total_Gigabytes_in_files         " + str(total_Gigabytes_in_files));
    log_this("INFO",g_routine_name,"FILES_STAT NUM_BATCHES_CREATED              " + str(num_batches_created));
    log_this("INFO",g_routine_name,"FILES_STAT NUM_PROCESSES_CREATED            " + str(num_processes_created));
    log_this("INFO",g_routine_name,"FILES_STAT MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN " + str(MAX_SIMULTANEOUS_SUB_PROCESSES_TO_RUN));
    log_this("INFO",g_routine_name,"FILES_STAT NUM_FILES_DOWNLOADED             " + str(num_files_downloaded));
    log_this("INFO",g_routine_name,"FILES_STAT Batch_size                       " + str(NUM_FILES_TO_PROCESS));

    if (num_files_downloaded > 0):
        average_processing_time = elapsed_in_seconds / num_files_downloaded;
        log_this("INFO",g_routine_name,"TIME_STAT Seconds_Average_Processing " + str("{:0.2f}".format(average_processing_time)));

#    log_this("INFO",g_routine_name,"TIME_STAT Time_Total_Download_If_Sequential " + str(time_total_download));
    log_this("INFO",g_routine_name,"TIME_STAT Seconds_Elapsed           " + str("{:.2f}".format(elapsed_in_seconds))); 
    log_this("INFO",g_routine_name,"TIME_STAT Minutes_Elapsed           " + str(elapsed_in_minutes));

    g_routine_name = "generic_level2_downloader";
    localtime = get_local_pdt_time();
    end_processing_time = strftime("%a %b %d %H:%M:%S %Y",localtime);#datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Y");
    log_this("INFO",g_routine_name,"BEGIN_PROCESSING_TIME "  + begin_processing_time);
    log_this("INFO",g_routine_name,"END_PROCESSING_TIME   "  + end_processing_time + " SECONDS " + str("{:.2f}".format(elapsed_in_seconds)) + " MINUTES " + str(elapsed_in_minutes) + " NUM_FILES " + str(num_files_read) + " OUT_OF " + str(num_sst_sst4_files));

    return (o_download_driver_status);

#------------------------------------------------------------------------------------------------------------------------
# PARENT_PROCESS: Do some house keeping to keep the indices and the number of jobs dispatched and the jobs info dispatched in check.
# Note that if doing parallel processing, the jobs are farmed out by the subprocesses and it is difficult to keep track of how long the jobs 
# take so we cannot use a timer here.
def parent_house_keeping_of_ledger(master_ledger,
                                   num_filenames_dispatched_per_batch,
                                   all_of_lines_to_download,
                                   num_subprocesses_started,
                                   working_on_this_batch,
                                   DUMMY_PROCESS_ID):
                               
    debug_module = "parent_house_keeping_of_ledger:";
    debug_mode   = 0;
    o_num_subprocesses_started = 0;
    o_child_pid = DUMMY_PROCESS_ID;  # Set this child process id to something we don't normally expect to represent running the parent process.

    (o_read_status,all_of_lines_to_download) = read_from_master_ledger(master_ledger,
                                                                       num_filenames_dispatched_per_batch,
                                                                       all_of_lines_to_download);
    if (debug_mode):
        log_this("DEBUG",debug_module,"MULTI_PROCESS_IN_PARENT_PROCESS SCALAR_ALL_OF_LINES_TO_DOWNLOAD " + str(len(all_of_lines_to_download)));

    log_this("INFO",debug_module,"MULTI_PROCESS_IN_PARENT_PROCESS " +  working_on_this_batch);
    o_num_subprocesses_started = num_subprocesses_started + 1;     # We assume that the sub process has been started for each loop.

    log_this("INFO",debug_module,"MULTI_PROCESS_IN_PARENT_PROCESS NUM_SUBPROCESSES_STARTED " + str(o_num_subprocesses_started) + " NUM_FILENAMES_DISPATCHED_PER_BATCH " + str(num_filenames_dispatched_per_batch));

    return(o_child_pid,
           o_num_subprocesses_started,
           all_of_lines_to_download);


#------------------------------------------------------------------------------------------------------------------------
# PARENT_PROCESS: Do some house keeping to keep the indices and the number of jobs dispatched and the jobs info dispatched in check.
# Note that if doing parallel processing, the jobs are farmed out by the subprocesses and it is difficult to keep track of how long the jobs 
# take so we cannot use a timer here.

from file_utils_for_interprocess_communication import read_master_file_for_interprocess_communication;
def parent_house_keeping_of_ledger_via_file(i_scratch_area,
                                            i_processing_level,
                                            i_processing_type,
                                            num_filenames_dispatched_per_batch,
                                            all_of_lines_to_download,
                                            num_subprocesses_started,
                                            working_on_this_batch,
                                            DUMMY_PROCESS_ID):
    
    debug_module = "parent_house_keeping_of_ledger_via_file:";
    debug_mode   = 0;
    o_num_subprocesses_started = 0;
    o_child_pid = DUMMY_PROCESS_ID;  # Set this child process id to something we don't normally expect to represent running the parent process.

    (lines_to_download_per_batch) = read_master_file_for_interprocess_communication(i_scratch_area,
                                                                                    i_processing_level,
                                                                                    i_processing_type);
    all_of_lines_to_download.extend(lines_to_download_per_batch);
    if (debug_mode):
        log_this("DEBUG",debug_module,"MULTI_PROCESS_IN_PARENT_PROCESS SCALAR_ALL_OF_LINES_TO_DOWNLOAD " + str(len(all_of_lines_to_download)));

    log_this("INFO",debug_module,"MULTI_PROCESS_IN_PARENT_PROCESS " +  working_on_this_batch);
    o_num_subprocesses_started = num_subprocesses_started + 1;     # We assume that the sub process has been started for each loop.

    log_this("INFO",debug_module,"MULTI_PROCESS_IN_PARENT_PROCESS NUM_SUBPROCESSES_STARTED " + str(o_num_subprocesses_started) + " NUM_FILENAMES_DISPATCHED_PER_BATCH " + str(num_filenames_dispatched_per_batch));

    return(o_child_pid,
           o_num_subprocesses_started,
           all_of_lines_to_download);

#------------------------------------------------------------------------------------------------------------------------
def print_batch_of_jobs_content(child_ledger,
                                master_ledger,
                                num_filenames_dispatched_per_batch,
                                all_of_lines_to_download,
                                batch_of_lines_to_download,
                                num_batches_created,
                                debug_module):

    # Not sure if the ledgers are needed here.  TODO: Figure it out.

    name_count=0;
    names_printed_for_test = 0;
    for one_line in batch_of_lines_to_download:
        name_count = name_count + 1;
        names_printed_for_test = names_printed_for_test + 1;
        print(debug_module + "NAMES_PRINTED_FOR_TEST " + str('{:05d}'.format(names_printed_for_test)) + " NUM_BATCHES_CREATED " + str('{:02d}'.format(num_batches_created)) + " NAME_COUNT " + str('{:05d}'.format(name_count)) + " " + one_line.rstrip('\n'));

    return(1);

#------------------------------------------------------------------------------------------------------------------------
# Write to ledger to register the jobs in i_batch_of_lines_to_download variable.  Note that this should be done before the fork() call so we can register immediately instead of letting
# the sub process register it in case it cannot be fork.
def register_jobs_in_batch(i_child_ledger,
                           i_batch_of_lines_to_download,
                           i_top_level_output_directory,
                           i_processing_level,
                           i_processing_type,
                           i_num_filenames_dispatched_per_batch,
                           i_num_batches_created):

    debug_module = "register_jobs_in_batch:";
    debug_mode   = 0;

    name_count=0;

    for one_line in i_batch_of_lines_to_download:
        name_count = name_count + 1;
        one_line = one_line.rstrip('\n');
        if (debug_mode):
            print(debug_module + "WRITE_TO_LEDGER:NUM_BATCHES_CREATED " + str("{0:02d}".format(i_num_batches_created)) + " NAME_COUNT " + str("{0:03d}".format(name_count)) + " " + one_line);

        i_child_ledger.write(one_line + "\n");
        i_child_ledger.flush();

        # Register the job by creating an empty file on the file system in the .hidden directory.
        
        (o_register_status,o_temporary_location_of_downloaded_file) = settings.g_gdjm.register_this_job(one_line,
                                                                                                        i_top_level_output_directory,
                                                                                                        i_processing_level,
                                                                                                        i_processing_type);
        if (debug_mode):
            print(debug_module +"REGISTER_TO_LEDGER:REGISTER_STATUS " + str(o_register_status) + " TEMPORARY_LOCATION_OF_DOWNLOADED_FILE " + o_temporary_location_of_downloaded_file);

    return(1);

#------------------------------------------------------------------------------------------------------------------------
# Write to file on disk to register the jobs in i_batch_of_lines_to_download variable.  Note that this should be done before the fork() call so we can register immediately instead of letting
# the sub process register it in case it cannot be fork.
from file_utils_for_interprocess_communication import write_child_file_for_interprocess_communication;
from file_utils_for_interprocess_communication import delete_file_for_interprocess_communication;
def register_jobs_in_batch_via_file(i_scratch_area,
                           i_batch_of_lines_to_download,
                           i_top_level_output_directory,
                           i_processing_level,
                           i_processing_type,
                           i_num_filenames_dispatched_per_batch,
                           i_num_batches_created):

    debug_module = "register_jobs_in_batch_via_file:";
    debug_mode   = 0;

    name_count=0;
    
    t0 = time.time();
    rand_int = random.randint(1, 1000)
    for one_line in i_batch_of_lines_to_download:
        name_count = name_count + 1;
        one_line = one_line.rstrip('\n');
        if (debug_mode):
            print(debug_module + "WRITE_TO_LEDGER:NUM_BATCHES_CREATED " + str("{0:02d}".format(i_num_batches_created)) + " NAME_COUNT " + str("{0:03d}".format(name_count)) + " " + one_line);

        donotcare = write_child_file_for_interprocess_communication(i_scratch_area,
                                                                    i_processing_level,
                                                                    i_processing_type,
                                                                    one_line,
                                                                    rand_int);
        # Register the job by creating an empty file on the file system in the .hidden directory.

        (o_register_status,o_temporary_location_of_downloaded_file) = settings.g_gdjm.register_this_job(one_line,
                                                                                                        i_top_level_output_directory,
                                                                                                        i_processing_level,
                                                                                                        i_processing_type);
        if (debug_mode):
            print(debug_module +"REGISTER_TO_LEDGER:REGISTER_STATUS " + str(o_register_status) + " TEMPORARY_LOCATION_OF_DOWNLOADED_FILE " + o_temporary_location_of_downloaded_file);
    t1 = time.time();
    if (os.getenv('CRAWLER_SEARCH_DOWNLOADER_REGISTER_ONLY_FLAG','') == 'true'):
         print(debug_module +"CRAWLER_SEARCH_DOWNLOADER_REGISTER_ONLY_FLAG is true.  Program exiting.");
         print(debug_module +"JOBS_REGISTERED " + str(name_count) + " IN " + str("{0:.2f}".format(t1-t0)) + " SECONDS");
         delete_file_for_interprocess_communication();
         exit(0);

    return(1);

#------------------------------------------------------------------------------------------------------------------------
# Function collect the jobs that would have been dispatched by reading from the master_ledger.
def read_from_master_ledger(master_ledger,
                            num_filenames_dispatched_per_batch,
                            all_of_lines_to_download):

    debug_module = "read_from_master_ledger:";
    debug_mode   = 0;

    o_read_status = 1;

    if (debug_mode):
        print(debug_module + "all_of_lines_to_download",all_of_lines_to_download);
        print(debug_module + "len(all_of_lines_to_download)",len(all_of_lines_to_download));

    # It is possible that the ledger has not been created or close, we make a sanity check and return.
    if (master_ledger is None):
        log_this("WARN",debug_module,"Variable master_ledger is None.   Cannot read from ledger.");
        return(o_read_status,all_of_lines_to_download);

    if (debug_mode):
        log_this("DEBUG",debug_module,"READ_FROM_MASTER_LEDGER STARTING");

    for job_completion_index in range(num_filenames_dispatched_per_batch):
        one_line = master_ledger.readline();
        one_line = one_line.rstrip('\n');
        if (debug_mode):
            print(debug_module + "one_line",one_line);
        all_of_lines_to_download.append(one_line);
    # end for job_completion_index in range(num_filenames_dispatched_per_batch-1):

    if (debug_mode):
        log_this("DEBUG",debug_module,"MULTI_PROCESS_IN_PARENT_PROCESS SCALAR_ALL_OF_LINES_TO_DOWNLOAD " + str(len(all_of_lines_to_download)));
        log_this("DEBUG",debug_module,"READ_FROM_MASTER_LEDGER STOPPING");

    return(o_read_status,all_of_lines_to_download);

#------------------------------------------------------------------------------------------------------------------------
# Given a list of files to download, this function: 
#   1.  Retrieve a batch names that share the same time frame.
#   2.  Note that not all dataset share the same naming convention.  This is where care should be taken to inspect whether the
#       code can handle all interested dataset processed by this script.

def get_names_of_same_time(list_of_files_to_download,
                           index_to_sst_sst4_list,
                           batch_size,
                           num_files_downloaded,
                           NUM_FILES_TO_PROCESS):

    debug_module = "get_names_of_same_time:";
    debug_mode   = 0;

    # Return variable(s):

    o_similiar_names_list            = [];
    o_updated_index_to_list_of_files = index_to_sst_sst4_list;
    o_similiar_names_fetched         = 0;

    num_sst_sst4_files        = len(list_of_files_to_download);
    granule_name_base = "";
    granule_name_retrieved_flag = 0;
    done_with_similiar_names_search_flag = 0;

    if (debug_mode):
        print(debug_module + "len(list_of_files_to_download)",len(list_of_files_to_download));
        print(debug_module + "index_to_sst_sst4_list",index_to_sst_sst4_list);
        print(debug_module + "batch_size",batch_size);
        print(debug_module + "num_files_downloaded",num_files_downloaded);
        print(debug_module + "NUM_FILES_TO_PROCESS",NUM_FILES_TO_PROCESS);
        print(debug_module + "o_updated_index_to_list_of_files",o_updated_index_to_list_of_files);

    while ((o_updated_index_to_list_of_files < num_sst_sst4_files) and (not done_with_similiar_names_search_flag)):

        # Get each line and save it in output array.
        one_line = list_of_files_to_download[o_updated_index_to_list_of_files].strip().rstrip("\n");

        # It is possible for the line to an empty string.  We look for it.
        if (len(one_line) == 0):
            log_this("WARN",debug_module,"Cannot process an empty string.  Skipping line at index " + str(o_updated_index_to_list_of_files));
            o_updated_index_to_list_of_files = o_updated_index_to_list_of_files + 1;
            continue;

        if (debug_mode):
            print(debug_module + "o_updated_index_to_list_of_files",o_updated_index_to_list_of_files,"one_line[",one_line,"]");

        # Get the granule start time by parsing for A2015039002000
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_OC.nc 6a684553a4b0f108b1d2d32dd7d029791bc6581f
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_SST4.nc 2c4959c1cacdc66b5d375e95ac56a34074b411d0
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_SST.nc 1f961468e03eb881aa92e7a72128600ff95ff3f1
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/Q2014215012500.L2_EVQL_V4.2.1.bz2 c7d3329a4521b8b2c993f4ab85ba80cebadab2b5
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/Q2014215012500.L2_EVSCI_V4.2.0.bz2 385a7a83e5507e188e95ba567dcdbe9c04e958a4
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/Q2014215030300.L2_EVQL_V4.2.1.bz2 645b4531bc4701fac1995dd2f5876d6678182307
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/Q2015158.L3m_DAY_SCI_V4.0_density_1deg.bz2 ce92e8a7a320c7a405af303d3f8a993c9271ec0f

        example_token = "A2015039002000";
        example_length = len(example_token);

        # Because the index() function raise an an exception if the string is not found, we place the index() function with a try/except clause.
        which_level_search_successful = "";
        try:
            # Look for .L2 in the string.
            if (debug_mode):
                print(debug_module + "LOOKING_FOR_L2 IN_LINE " + one_line);
            index_to_level_number_token = one_line.index(".L2");
            which_level_search_successful = ".L2";
        except: # catch *all* exceptions 
            try:
                # Look for .L3 in the string.
                if (debug_mode):
                    print(debug_module + "FAILED_L2_LOOKING_FOR_L3 IN_LINE " + one_line);
                index_to_level_number_token = one_line.index(".L3");
                which_level_search_successful = ".L3";
            except: # catch *all* exceptions
                try:
                    # Look for .L4 in the string.
                    if (debug_mode):
                        print(debug_module + "FAILED_L3_LOOKING_FOR_L4 IN_LINE " + one_line);
                    index_to_level_number_token = one_line.index(".L4");
                    which_level_search_successful = ".L4";
                except: # catch *all* exceptions
                    print(debug_module + "ERROR: This script only support .L2, .L3, or .L4 in the URL.  Cannot continue.");
                    print(debug_module + "ERROR: Current line " + one_line);
                    exit(0);

        # Because the granule name may be shorter than the example_token, we have to check to see if it indeed is shorter.

        filename_only = os.path.basename(one_line);
        index_to_level_name = filename_only.index(which_level_search_successful);

        # Update the new length since our name may be shorter than the example.
        if (index_to_level_name <= example_length):
            example_length = index_to_level_name;

        partial_name = one_line[index_to_level_number_token-example_length:index_to_level_number_token]; # Copy from A up to .L2_LAC: A2015039002000;

        if (debug_mode): 
            print(debug_module + "    one_line [",one_line,"]");
            print(debug_module + "    index_to_level_number_token",index_to_level_number_token);
            print(debug_module + "    partial_name [",partial_name,"]");

        if (not granule_name_retrieved_flag):
            granule_name_base = partial_name;
            granule_name_retrieved_flag = 1;

        # Save the name if if it is similiar to the granule_name_base, e.g A2015039002000.L2_LAC_SST4.nc and A2015039002000.L2_LAC_SST.nc will be saved.  
        if (partial_name == granule_name_base):
            if (debug_mode): 
                print(debug_module + "APPEND:",one_line);
            o_similiar_names_list.append(one_line);

            o_similiar_names_fetched         = o_similiar_names_fetched + 1;
            o_updated_index_to_list_of_files = o_updated_index_to_list_of_files + 1;

            # Note: We only increment o_updated_index_to_list_of_files if the name are the same otherwise we would have indexed one value pass.
            #       The next time the function get_names_of_same_time() gets called, the value of index_to_sst_sst4_list is correct to get the next batch of similar name.

            # Check to see if we have reached the batch_size.  If we have, we are done here.
            if (o_similiar_names_fetched >= batch_size):
                if (debug_mode): 
                    print(debug_module + "    o_similiar_names_fetched",o_similiar_names_fetched,"IS_GREATER_OR_EQUAL_TO","batch_size",batch_size);
                    print(debug_module + "    setting done_with_similiar_names_search_flag to 1");
                done_with_similiar_names_search_flag = 1;

            # Check to see if we have fetched more than requested.

            if (o_updated_index_to_list_of_files >= NUM_FILES_TO_PROCESS):
                if (debug_mode):
                    print(debug_module + "    o_updated_index_to_list_of_files",o_updated_index_to_list_of_files,"IS_GREATER_OR_EQUAL_TO","NUM_FILES_TO_PROCESS",NUM_FILES_TO_PROCESS);
                    print(debug_module + "    setting done_with_similiar_names_search_flag to 1");
                done_with_similiar_names_search_flag = 1;

        else:
            # We have detected the name change so we are done with this granule_name_base
            if (debug_mode): 
                print(debug_module + "    partial_name",partial_name,"IS_DIFFERENT_THAN","granule_name_base",granule_name_base);
                print(debug_module + "    setting done_with_similiar_names_search_flag to 1");
            done_with_similiar_names_search_flag = 1;
        # end if (partial_name == granule_name_base)
    # end while ((o_updated_index_to_list_of_files < num_sst_sst4_files) and (not done_with_similiar_names_search_flag))

    if (debug_mode): 
       print(debug_module + "o_similiar_names_list",o_similiar_names_list);
       print(debug_module + "o_similiar_names_fetched",o_similiar_names_fetched);
       print(debug_module + "o_updated_index_to_list_of_files",o_updated_index_to_list_of_files);

    return(o_similiar_names_list,o_similiar_names_fetched,o_updated_index_to_list_of_files);

#------------------------------------------------------------------------------------------------------------------------
# Given a list of files to download, this function: 
#
#   1.  Retrieve a batch of names up to batch_size or the group of files with the same time frame and return the list of names.
#   2.  Update the next index to the list of files to download.
#
# Some notes:
#
#   1.  The download list has a special order.  OC file is before SST.  SST4 file is before SST.  
#   2.  The names are sorted to have the same time frame.
#   3.  We will keep the files with the same time frame together so we can preserve the order of the download.
#
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039000000.L2_LAC_OC.nc 3da54bc4b9bc2ce8abcf7f6b58c54372ba483929
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039000000.L2_LAC_SST.nc 5424a547a19b30d7319b702eaa851775a5e2545a
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039000500.L2_LAC_OC.nc 05ef1fd0b5557887d9f2d17d9ce572f0c7dbf257
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039000500.L2_LAC_SST.nc ab4d1632368dba4c147d44ddfc4978f27b551663
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039001000.L2_LAC_OC.nc f2c0f518db639229cfd0b9e2281774d5541ca21b
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039001000.L2_LAC_SST.nc 0a4d6283a23b7f384571c478f6ed31a8b32dd9a7
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039001500.L2_LAC_OC.nc 64dbf8188c03fa85f5ed71e03873e745650a1b23
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039001500.L2_LAC_SST.nc 3a7e51b902d5e518fc6a62b80ddf4e1dacb1b76d
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_OC.nc 6a684553a4b0f108b1d2d32dd7d029791bc6581f
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_SST4.nc 2c4959c1cacdc66b5d375e95ac56a34074b411d0
#           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_SST.nc 1f961468e03eb881aa92e7a72128600ff95ff3f1

def get_batch_of_lines(list_of_files_to_download,
                       index_to_sst_sst4_list,
                       batch_size,
                       num_files_downloaded,
                       NUM_FILES_TO_PROCESS):

    debug_module = "get_batch_of_lines:";
    debug_mode   = 0;

    # Return variable(s):

    o_batch_of_lines_to_download         = [];  # A list of lines up to the batch_size of files to download.
    o_num_filenames_dispatched_per_batch = 0;
    o_updated_index_to_list_of_files     = index_to_sst_sst4_list;  # Start indexing with this value.

    num_sst_sst4_files        = len(list_of_files_to_download);
    o_num_files_should_have_downloaded = num_files_downloaded;

    if (debug_mode):
        log_this("INFO",debug_module,"len(o_batch_of_lines_to_download) " + str(len(o_batch_of_lines_to_download)) + " o_updated_index_to_list_of_files " + str(o_updated_index_to_list_of_files) + " o_num_files_should_have_downloaded " + str(o_num_files_should_have_downloaded));

    similiar_names_list = [];
    similiar_names_fetched = 0;

    # The logic of the while loop is somewhat tricky.  Within the while loop, we call get_names_of_same_time() to get a group of file names
    # that share the same time frame so they can be downloaded sequentially.  The reason why we want the files to be downloaded in a certain
    # order so come time for them to be combined together, files will be available as they are expected.
    # So the looping criteria are we will loop until:
    #   1.  The index reaches the number of names in the list or 
    #   2.  The number of names fetched exceeds the upper limit, or 
    #   3.  The number of names fetched exceeds the batch size. 

    while (((o_updated_index_to_list_of_files < num_sst_sst4_files) and (o_num_files_should_have_downloaded < NUM_FILES_TO_PROCESS)) and 
          (o_num_filenames_dispatched_per_batch < batch_size)):

        if (debug_mode):
            print(debug_module +"BEFORE:o_updated_index_to_list_of_files " + str(o_updated_index_to_list_of_files));

        # Get a group of names that share the same time frame.  It can be 1, 2 or 3 names depends on what's in the list.
        #  Example:
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_OC.nc 6a684553a4b0f108b1d2d32dd7d029791bc6581f
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_SST4.nc 2c4959c1cacdc66b5d375e95ac56a34074b411d0
        #           http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2015039002000.L2_LAC_SST.nc 1f961468e03eb881aa92e7a72128600ff95ff3f1

        (similiar_names_list,similiar_names_fetched,o_updated_index_to_list_of_files) = get_names_of_same_time(list_of_files_to_download,
                                                                                                               o_updated_index_to_list_of_files,
                                                                                                               batch_size,
                                                                                                               num_files_downloaded,
                                                                                                               NUM_FILES_TO_PROCESS);
        # Add these names to the list of names to download per batch.
        # Because Python append() function add a list to a list, we want to use the extend() to add the items in similiar_names_list to o_batch_of_lines_to_download
        o_batch_of_lines_to_download.extend(similiar_names_list);

        if (debug_mode):
             log_this("INFO",debug_module,"NUM_NAMES_ADDED " + str('{:05d}'.format(similiar_names_fetched)) + " NEW_SIZE");
             log_this("INFO",debug_module,"NAMES_ADDED     " + str(similiar_names_list));

        # Update all the indices so we know when to quit the while loop and what to return the callee.

        o_num_files_should_have_downloaded   = o_num_files_should_have_downloaded   + similiar_names_fetched;
        o_num_filenames_dispatched_per_batch = o_num_filenames_dispatched_per_batch + similiar_names_fetched;
        if (debug_mode):
            print(debug_module + "AFTER:o_num_filenames_dispatched_per_batch " + str(o_num_filenames_dispatched_per_batch) + " batch_size " + str(batch_size));
    # end while (((o_updated_index_to_list_of_files < num_sst_sst4_files) and (o_num_files_should_have_downloaded < NUM_FILES_TO_PROCESS)) 
    #      (o_num_filenames_dispatched_per_batch < batch_size)):

    if (debug_mode):
        log_this("INFO",debug_module,"len(o_batch_of_lines_to_download) " + str(len(o_batch_of_lines_to_download)) + " o_updated_index_to_list_of_files " + str(o_updated_index_to_list_of_files) + " o_num_files_should_have_downloaded " + str(o_num_files_should_have_downloaded));
        log_this("INFO",debug_module,"o_batch_of_lines_to_download) " + str(o_batch_of_lines_to_download));

#    print "exit#0001";
#    sys.exit(0);
    return (o_batch_of_lines_to_download,o_updated_index_to_list_of_files,o_num_files_should_have_downloaded,o_num_filenames_dispatched_per_batch);

#------------------------------------------------------------------------------------------------------------------------
# After a batch of sub processes have started executing, we call this function to wait for them to complete.
# Because the child_pipe is being written to by each sub process, the read function of the master pipe is block, i.e.
# will wait for the child to write to the pipe.

def inspect_running_subprocesses_for_completion(i_master_pipe,
                                                i_num_processes_started_from_main):

    o_num_files_read = 0;
    o_time_per_batch = 0.0; 
    o_total_Bytes_in_files = 0; 

    debug_module = "inspect_running_subprocesses_for_completion:";
    debug_module = "inspect_running_subprocesses_for_completion:";
    debug_mode  = 0;

    one_line = "";
    num_lines_read = 0;
    loop_is_done_flag = 0;

    # This is what each line looks like:
    #
    # /data/dev/scratch/qchau/IO/data/MODIS_AQUA_L2_SST_OBPG/A2015039000500.L2_LAC_OC.nc DOWNLOAD_SUCCESS 5 73012805

    DOWNLOAD_NAME_INDEX   = 0;
    DOWNLOAD_STATUS_INDEX = 1;
    DOWNLOAD_TIME_INDEX   = 2;
    FILE_SIZE_INDEX       = 3;

    time_wait_start = time.time(); 
    if (debug_mode):
        log_this("INFO",debug_module,"READING_FROM_MASTER_PIPE");

    while (not loop_is_done_flag):
        if (debug_mode):
            print(debug_module + "WAITING FOR CHILD_PIPE TO FINISH WRITING...");
        one_line = i_master_pipe.readline(); # Read from the master pipe we created earlier.  Note this read is blocked so if the child does not write, we cannot read.

        # Check to see how long we have waited to get the first line from the pipe.
        if (num_lines_read == 0):
            time_wait_stop = time.time(); 
            time_wait_duration = time_wait_stop - time_wait_start;
            if (debug_mode):
                log_this("INFO",debug_module,"READING_FROM_MASTER_PIPE WAIT_DURATION_IN_SECONDS " + "{0:.2f}".format(time_wait_duration));

        one_line = one_line.strip('\n');  # Remove the carriage return.

        tokens = re.split('\s+', one_line)

        # Becareful that we have enough tokens from one_line.
        if (debug_mode):
           token_index = 0;
           for one_token in (tokens):
               log_this("DEBUG",debug_module,"tokens[" + str(token_index) +"] [" + one_token + "]"); 
               token_index = token_index + 1;

        # Do a sanity check and print a warning if the status (2nd token) is "DOWNLOAD_FAILURE"
        if (tokens[DOWNLOAD_STATUS_INDEX] == "DOWNLOAD_FAILURE"):
            log_this("ERROR",debug_module,"FILE_DOWNLOADED " + tokens[DOWNLOAD_NAME_INDEX] + " FILE_STATUS " + tokens[DOWNLOAD_STATUS_INDEX]);

        # Parse the line processed to get the time it took to download that particular file.
        o_time_per_batch = o_time_per_batch + float(tokens[DOWNLOAD_TIME_INDEX]);

        # Sum up the sizes of all files downloaded.
        o_total_Bytes_in_files = o_total_Bytes_in_files + int(tokens[FILE_SIZE_INDEX]);

        num_lines_read += 1;

        if (debug_mode):
            log_this("INFO",debug_module,str(num_lines_read) + " [" +  one_line + "]");

        # An empty string signifies that we are done with reading.  There shouldn't be any more lines.
        if (one_line == ""):
            loop_is_done_flag = 1;

        # If we have read up to the number of sub processes started, we are done, otherwise this while loop will wait forever.
        if (num_lines_read == i_num_processes_started_from_main):
            loop_is_done_flag = 1;

    # end while (not loop_is_done_flag):

    if (debug_mode):
        log_this("INFO",debug_module,"i_master_pipe [" + str(i_master_pipe) + " i_num_processes_started_from_main [" + str(i_num_processes_started_from_main) + "] num_lines_read [" + str(num_lines_read) + "]");

    o_num_files_read = num_lines_read; 

    return(o_num_files_read,
           o_time_per_batch,
           o_total_Bytes_in_files);



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

    debug_module = "generic_level2_downloader_driver_historical_using_process:";
    debug_mode   = 1;

    i_filelist_name = os.getenv('OBPG_RUNENV_RESOURCES_HOME','') + "/modis_aqua_filelist.txt.daily_2016_008_date_2016_01_08";
    i_separator_character = "SPACE";
    i_processing_level = "L2";
    i_processing_type = "MODIS_A";
    i_top_level_output_directory = "/data/dev/scratch/qchau/IO/data";
    i_num_files_to_download = 1;
    i_sleep_time_in_between_files = 0;
    i_move_filelist_file_when_done = "no";
    i_perform_checksum_flag = "no";
    i_today_date = "17_02_01_13_39_00";
    i_job_full_name = None; 
    i_test_run_flag = None;

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
