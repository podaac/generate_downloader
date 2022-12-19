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
#
# Python script to download one file at a time.  The input to perform_download_only() function  can either be a string containing parameters separated by comma
#
#    perform_download_only,"http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016293000000.L2_SNPP_SST.nc",fb5ea83a4a35268a57ebf6a39fe296a1c06f8278
#
# or by the two separate actual parameters: i_full_pathname_to_download, i_checksum_value

import datetime
import getopt
import os
import re
import requests
from requests.adapters import HTTPAdapter
import sys
import time

from log_this import log_this;

# Get global variables.
import settings

from const import CONST_PERFORM_DOWNLOAD_ONLY_MAX_ATTEMPTS, CONST_PERFORM_DOWNLOAD_ONLY_MAX_SLEEP_IN_BETWEEN_WAITS;

# set a default chunk size in case files are approaching 2GB :  2^20 chosen for ~1M and integer multipler of 65536 (2^16, magic storage number)
DEFAULT_CHUNK_SIZE = 1048576

# From:  OBPG, with thanks.  Requests session object used to keep connections 
dnldSession = None

def getSession(verbose=0, ntries=5):
    global dnldSession

    if not dnldSession:
        # turn on debug statements for requests
        if verbose > 1:
            log_this("INFO",debug_module,"Creating download session")

        dnldSession = requests.Session()
        dnldSession.mount('https://', HTTPAdapter(max_retries=ntries))

        if verbose:
            log_this("INFO",debug_module,"Download session started")
    else:
        if verbose > 1:
            log_this("INFO",debug_module,"Reusing existing session")

    return dnldSession


# From OBPG, with thanks.  Individual download request. 

def httpdl(https_server_request, localpath='.', outputfilename=None, ntries=5,
           timeout=36., verbose=0,
           chunk_size=DEFAULT_CHUNK_SIZE):

    status = 0

    global dnldSession

    getSession(verbose=verbose, ntries=ntries)

    with dnldSession.get(https_server_request, stream=True, timeout=timeout) as req:
        print(f"DOWNLOAD REQUEST: {req} and {req.headers}")
        req.raise_for_status()
        ctype = req.headers.get('Content-Type')
        if req.status_code in (400, 401, 403, 404, 416):
            status = req.status_code
        elif ctype and ctype.startswith('text/html'):
            status = 401
        else:
            if not os.path.exists(localpath):
                os.umask(000)
                os.makedirs(localpath, 0o777)    # NET edit. (Allow removal of directory on disk)

            if not outputfilename:
                cd = req.headers.get('Content-Disposition')
                if cd:
                    outputfilename = re.findall("filename=(.+)", cd)[0]
                else:
                    outputfilename = urlStr.split('/')[-1]
            
            ofile = os.path.join(localpath, outputfilename)

            with open(ofile, 'wb') as fd:
                for chunk in req.iter_content(chunk_size=chunk_size):
                    if chunk: # filter out keep-alive new chunks
                        fd.write(chunk)
                        
    return status



def perform_download_only(instanceOf, *args):

    # Given a download URL and the SHA-1 checksum, this function will attempt to download the file and validate the checksum.
    debug_module = "perform_download_only:";
    debug_mode   = 1;

    if (os.getenv("CRAWLER_SEARCH_DEBUG_FLAG") == "true"):
        debug_mode   = 1;

    o_download_status = 1;  # A value of 1 if successful or 0 if failed.
    o_time_spent_in_downloading_and_move = 0.0;

    i_checksum_value = None;  # Set to None in case it is not provided.
    i_test_run_flag  = None;  # Set to None in case it is not provided.

    if (instanceOf == "job_string"):
        # If the first parameter is a job_string, we parse the next 3 elements from args string.
        CONST_FUNCTION_NAME   = 0;
        CONST_OBPG_FILE_URL   = 1;
        CONST_CHECKSUM_VALUE  = 2;
        i_similiar_jobs_for_one_file = args[0];
        job_parameters_splitted_array = i_similiar_jobs_for_one_file.split(',');
        i_function_name     = job_parameters_splitted_array[CONST_FUNCTION_NAME];
        i_full_pathname_to_download = job_parameters_splitted_array[CONST_OBPG_FILE_URL].strip("'").strip('"');
        i_checksum_value = job_parameters_splitted_array[CONST_CHECKSUM_VALUE];
    else:
        # For instance other than "job_string", each argument is positional.
        i_function_name     = instanceOf;

        # We start with 0 since first 2 parameters of "python perform_download_only.py call_direct 0 1 https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016293000000.L2_SNPP_SST.nc /data/dev/scratch/qchau/IO/data/VIIRS_L2_SST_OBPG/.hidden/V2016293000000.L2_SNPP_SST.nc /data/dev/scratch/qchau/IO/data/VIIRS_L2_SST_OBPG/V2016293000000.L2_SNPP_SST.nc 0.0 no"
        # were discarded by the call perform_download_only;
        #   argument index x (discarded) = perform_download_only.py
        #   argument index x (discarded) = call_direct
        #   argument index 0 = 0 (i_num_files_downloaded)
        #   argument index 1 = 1 (i_num_sst_sst4_files) and so on.
        arg_index = 0;

        if (debug_mode):
            print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
        i_num_files_downloaded                  = int(args[arg_index]);
        arg_index += 1;
        if (debug_mode):
            print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
        i_num_sst_sst4_files                    = int(args[arg_index]);
        arg_index += 1;
        if (debug_mode):
            print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
        i_full_pathname_to_download             = args[arg_index];
        arg_index += 1;
        if (debug_mode):
            print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
        i_temporary_location_of_downloaded_file = args[arg_index];
        arg_index += 1;
        if (debug_mode):
            print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
        i_final_location_of_downloaded_file     = args[arg_index];
        arg_index += 1;
        if (debug_mode):
            print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
        i_time_spent_in_downloading             = float(args[arg_index]);
        arg_index += 1;
        if (debug_mode):
            print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
        i_perform_checksum_flag                 = args[arg_index];
        arg_index += 1;
        
        # Only retrieve the i_test_run_flag if user provided.
        if (len(args) > 9):
            if (debug_mode):
                print(debug_module + "arg_index",arg_index,"args[arg_index]",args[arg_index]);
            i_test_run_flag                         = args[arg_index];

        if (debug_mode):
            print(debug_module + "instanceOf          [",instanceOf,"]");
            print(debug_module + "args",args,"type(args)",type(args));
            if (len(args) > 9):
                print("arg_index",arg_index,"args[arg_index]",args[arg_index])

    if (debug_mode):
        print(debug_module + "i_function_name     [" + i_function_name     + "]");
        print(debug_module + "i_num_files_downloaded",str(i_num_files_downloaded));
        print(debug_module + "i_num_sst_sst4_files",str(i_num_sst_sst4_files));
        print(debug_module + "i_full_pathname_to_download",i_full_pathname_to_download);
        print(debug_module + "i_temporary_location_of_downloaded_file",i_temporary_location_of_downloaded_file);
        print(debug_module + "i_final_location_of_downloaded_file",i_final_location_of_downloaded_file);
        print(debug_module + "i_time_spent_in_downloading",str(i_time_spent_in_downloading));
        print(debug_module + "i_perform_checksum_flag",i_perform_checksum_flag);
        print(debug_module + "i_checksum_value",i_checksum_value);
        print(debug_module + "i_test_run_flag",i_test_run_flag);

    ok_to_download_file_flag = 1;
    if (settings.g_use_file_locking_mechanism_flag):
        from nfs_lock_file_wrapper import nfs_lock_file_wrapper;
        (ok_to_download_file_flag, o_the_lock) = nfs_lock_file_wrapper(i_temporary_location_of_downloaded_file,None);

    if (ok_to_download_file_flag == 0):
        o_download_status = 0;  # A value of 1 if successful or 0 if failed.
        return(o_download_status,o_time_spent_in_downloading_and_move);

    # Parse the URL http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008002000.L2_LAC_SST.nc for the name only.

    filename_only = os.path.basename(i_full_pathname_to_download);

    # Do a sanity if the name part is an empty string.
    if (filename_only == ""):
        print("ERROR: Cannot parse file name from URL", i_full_pathname_to_download);
        o_download_status = 0;
        return(o_download_status);

    if (debug_mode):
        print(debug_module + "i_full_pathname_to_download",i_full_pathname_to_download);
        print(debug_module + "filename_only",filename_only);
        print(debug_module + "CONST_PERFORM_DOWNLOAD_ONLY_MAX_ATTEMPTS",CONST_PERFORM_DOWNLOAD_ONLY_MAX_ATTEMPTS);
        print(debug_module + "CONST_PERFORM_DOWNLOAD_ONLY_MAX_SLEEP_IN_BETWEEN_WAITS",CONST_PERFORM_DOWNLOAD_ONLY_MAX_SLEEP_IN_BETWEEN_WAITS);

    t0 = time.time()
    t1 = 0.0; 
    time_download = 0.0;

    # Attempt to download until we run out of attempts.
    attempt_count = 1;
    url_timeout   = 30;

    while attempt_count <= CONST_PERFORM_DOWNLOAD_ONLY_MAX_ATTEMPTS:
        log_this("INFO",debug_module,"BEGIN_FILE_DOWNLOADING" + " " + i_full_pathname_to_download + " " + "TO_FILE" + " " + i_temporary_location_of_downloaded_file);
        try:
            # Perform the download only.  httpdl also writes the file to temp location. 
            status = httpdl(i_full_pathname_to_download, localpath=i_temporary_location_of_downloaded_file)
            
            # Save the content read from urlopen() to a file.

            t1 = time.time()
            time_download = t1 - t0;
            # The download is a success, we can break out of the while loop immediately.
            break;
        # except requests.exceptions.Timeout as e:
        except requests.exceptions.ConnectTimeout as e:
            t1 = time.time()
            time_download = t1 - t0;

            log_this("ERROR",debug_module,"FILE_DOWNLOADING" + " " + i_temporary_location_of_downloaded_file + " " + "DOWNLOAD_ELAPSED" + str('{:5.2f}'.format(time_download)) + " " + "ATTEMPT_COUNT" + " " + str('{:03d}'.format(attempt_count)));
            attempt_count += 1;

            # Go to sleep to give the other process time to finish:

            log_this("ERROR",debug_module,"FILE_DOWNLOADING" + " " + i_temporary_location_of_downloaded_file + " " + "SLEEPING " + str(CONST_PERFORM_DOWNLOAD_ONLY_MAX_SLEEP_IN_BETWEEN_WAITS) + " " + "SECONDS");
            time.sleep(CONST_PERFORM_DOWNLOAD_ONLY_MAX_SLEEP_IN_BETWEEN_WAITS);
            
        except requests.exceptions.ConnectionError as e:
            t1 = time.time()
            time_download = t1 - t0;

            log_this("ERROR",debug_module,"FILE_DOWNLOADING" + " " + i_temporary_location_of_downloaded_file + " " + "DOWNLOAD_ELAPSED" + str('{:5.2f}'.format(time_download)) + " " + "ATTEMPT_COUNT" + " " + str('{:03d}'.format(attempt_count)));
            attempt_count += 1;

            # Go to sleep to give the other process time to finish:

            log_this("ERROR",debug_module,"FILE_DOWNLOADING" + " " + i_temporary_location_of_downloaded_file + " " + "SLEEPING " + str(CONST_PERFORM_DOWNLOAD_ONLY_MAX_SLEEP_IN_BETWEEN_WAITS) + " " + "SECONDS");
            time.sleep(CONST_PERFORM_DOWNLOAD_ONLY_MAX_SLEEP_IN_BETWEEN_WAITS);


    if (attempt_count <= CONST_PERFORM_DOWNLOAD_ONLY_MAX_ATTEMPTS):
        log_this("INFO",debug_module,"SUCCESS_FILE_DOWNLOADED " + i_temporary_location_of_downloaded_file + " DOWNLOAD_ELAPSED " + str('{:5.2f}'.format(time_download)) + " ATTEMPT_COUNT " +  str('{:03d}'.format(attempt_count)));
    else:
        o_download_status = 0;
        log_this("ERROR",debug_module,"FAILURE_FILE_DOWNLOADED " + i_temporary_location_of_downloaded_file + " DOWNLOAD_ELAPSED " + str('{:5.2f}'.format(time_download)) + " ATTEMPT_COUNT " + str('{:03d}'.format(attempt_count)));

    if (settings.g_use_file_locking_mechanism_flag):
        from nfs_unlock_file import nfs_unlock_file;
        nfs_unlock_file(o_the_lock,i_temporary_location_of_downloaded_file);

    o_time_spent_in_downloading_and_move = time_download;

    return(o_download_status,o_time_spent_in_downloading_and_move);

if __name__ == "__main__":
    # python perform_download_only.py call_direct 0 1 https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016293000000.L2_SNPP_SST.nc /data/dev/scratch/qchau/IO/data/VIIRS_L2_SST_OBPG/.hidden/V2016293000000.L2_SNPP_SST.nc /data/dev/scratch/qchau/IO/data/VIIRS_L2_SST_OBPG/V2016293000000.L2_SNPP_SST.nc 0.0 no 
    # python perform_download_only.py call_direct 0 1 https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016293000600.L2_SNPP_SST.nc /data/dev/scratch/qchau/IO/data/VIIRS_L2_SST_OBPG/.hidden/V2016293000600.L2_SNPP_SST.nc /data/dev/scratch/qchau/IO/data/VIIRS_L2_SST_OBPG/V2016293000600.L2_SNPP_SST.nc 0.0 no 

    debug_module = "perform_download_only:";
    debug_mode   = 0;

    # If we are running interactively, we need to call the init() function to make availables some global values.
    settings.init();

    function_name = "call_direct";
    i_full_pathname_to_download = sys.argv[4];

    print(debug_module + "Calling perform_download_only: i_full_pathname_to_download",i_full_pathname_to_download);
 
    perform_download_only(function_name,*(sys.argv[2:]));  # We pass the argv list start with 2: to make the first argument the i_num_files_downloaded, then i_num_sst_sst4_files


