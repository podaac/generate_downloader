#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$

import os

from log_this import log_this;

#------------------------------------------------------------------------------------------------------------------------
def delete_error_count(i_full_pathname):
    # Given a full pathname, find the associated error count file if it exists and delete it.
    #
    # A full path name to download looks like this.
    #
    #     https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/T2013150003500.L2_LAC_SST.bz2
    g_routine_name = "delete_error_count";

    debug_module = "delete_error_count:";
    debug_mode   = 0;

    if (os.getenv("CRAWLER_SEARCH_DEBUG_FLAG") == "true"):
        debug_mode   = 1;

    # Parse the full path name just for the name only.
    last_slash_pos = i_full_pathname.rindex("/");

    name_only = i_full_pathname[last_slash_pos+1:];   # The name_only variable should now be T2013150003500.L2_LAC_SST.bz2
    error_count_filename = name_only + ".error_count";
    # scratch_count_directory = os.getenv('HOME','') + "/scratch/";
    scratch_count_directory = os.getenv('DOWNLOAD_ERROR_COUNTS_LOCATION','') + "/"    # NET edit.
    if (not os.path.exists(scratch_count_directory)):
        try:
            print(debug_module + "create directory " + scratch_count_directory);
            os.umask(000)
            os.makedirs(scratch_count_directory, 0o777)    # NET edit. (Allow removal of directory on disk)
        except OSError as exception:
            print(debug_module + "WARN: Cannot create directory" + scratch_count_directory);

    error_count_directory = scratch_count_directory + "generic_download_error_counts/";
    if (not os.path.exists(error_count_directory)):
        try:
            print(debug_module + "create directory " + error_count_directory);
            os.umask(000)
            os.makedirs(error_count_directory, 0o777)    # NET edit. (Allow removal of directory on disk)
        except OSError as exception:
            print(debug_module + "WARN: Cannot create directory" + error_count_directory);

    full_error_count_pathname = error_count_directory + error_count_filename;

    if (debug_mode):
        print(debug_module + "full_error_count_pathname [" + full_error_count_pathname + "]");
    if (os.path.isfile(full_error_count_pathname)):
         os.remove(full_error_count_pathname);
         log_this("INFO",g_routine_name,"ERROR_COUNT_FILE_REMOVED " + full_error_count_pathname);

    return;

if __name__ == "__main__":
    debug_module = "delete_error_count:";
    debug_mode   = 0;

    i_full_pathname = "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/T2013150003500.L2_LAC_SST.bz2";
    name_only       = "T2013150003500.L2_LAC_SST.bz2";
    error_count_filename = name_only + ".error_count";
    scratch_count_directory = os.getenv('HOME','') + "/scratch/"; 
    error_count_directory   = scratch_count_directory + "generic_download_error_counts/"; 

    # Create the empty file.
    print("touch "  + error_count_directory + error_count_filename);
    os.system("touch "  + error_count_directory + error_count_filename);

    # Attempt to delete it.
    donotcare = delete_error_count(i_full_pathname);
