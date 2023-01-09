#***************************************************************************
#
# Copyright 2017, by the California Institute of Technology. ALL
# RIGHTS RESERVED. United States Government Sponsorship acknowledged.
# Any commercial use must be negotiated with the Office of Technology
# Transfer at the California Institute of Technology.
#
# @version $Id$
#
#****************************************************************************/
#
# Python script to fetch a checksum of a file from OBPG.

import datetime
import getopt
import os
import re
import sys
import time
import urllib.request, urllib.error, urllib.parse

from log_this import log_this;

from const import CONST_GETFILE_URI, CONST_SEARCH_URI;
from const import CONST_GET_FILE_CHECKSUM_MAX_ATTEMPTS, CONST_GET_FILE_CHECKSUM_MAX_SLEEP_IN_BETWEEN_WAITS;

def get_file_checksum(search_filter,
                      search_dtype,
                      search_sensor):

    # Parameter search_filter is typicall the actual file name we wish to fetch the SHA-1 checksum for.

    g_debug = 0;     # Change to 1 if want to see debug prints.
    g_module_name = 'get_file_checksum:'
    debug_module  = 'get_file_checksum:'

    if (os.getenv("CRAWLER_SEARCH_DEBUG_FLAG") == "true"):
        g_debug = 1

    getfile_uri     = CONST_GETFILE_URI;
    search_uri      = CONST_SEARCH_URI;
    search_std_only = "0" # Boolean, avoid non standard files
    search_as_file  = "1" # Boolean, get the result as a file.
    search_addurl   = "1" # Boolean, 1 will prepend "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to the file name.
    checksum_flag   = "1" # Boolean, 1 will result in the checksum of the file prepended to filename and a space.  This script will do some post processing to produce the correct output.
    search_sdate    = ""  # "2015-01-01" (must be inside double quotes)
    search_edate    = ""  # "2015-01-01" (must be inside double quotes)
    search_groupby  = ""  # {hourly,daily,weekly,monthly,yearly}

    o_checksum_part = ""; 

    # Define a list of names that this code will only look for and ignore anything else.
    # Add any new patterns you want to add here separate by '|' character.

    pattern_to_look_for = re.compile('_SST|_NSST|_SST3|_SST4|_OC|CHL|_EV|_QL|_SCI|_SOIL|cap');  # These regular expression should cover modis, aquarius, and viirs datasets.

    # Check to see if search_sdate and search_edate are provided and add them to the query_string.
    query_string = ""
    dtype_clause  = "";
    sensor_clause = "";

    if (search_dtype != None):
        dtype_clause = "&dtype=" + search_dtype;
    if (search_sensor != None):
        sensor_clause = "&sensor=" + search_sensor;

    if (search_sdate != "" and search_edate != ""): 
        query_string = search_uri + "?" + dtype_clause + "&search=" + search_filter + sensor_clause + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag + "&sdate=" + search_sdate + "&edate=" + search_edate
    else:
        query_string = search_uri + "?" + dtype_clause + "&search=" + search_filter + sensor_clause  + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag
    
    if (g_debug):
        # An example query string:
        #
        #   https://oceandata.sci.gsfc.nasa.gov/search/file_search.cgi?&search=Q20151582015164.L3b_R7_SCI_V4.0.main.bz2&std_only=0&results_as_file=1&addurl=1&cksum=1
        #
        # An example of the curl command with the query string in quotes (either single or double works):
        #
        #   curl 'https://oceandata.sci.gsfc.nasa.gov/search/file_search.cgi?&search=Q20151582015164.L3b_R7_SCI_V4.0.main.bz2&std_only=0&results_as_file=1&addurl=1&cksum=1'
        # An example output from the above curl command.  This code will have to parse the output array of strings.
        #
        # Your query generated 1 results, which are displayed below
        #
        # afef5b084efe493f41f0ef8dd24cd1e8dca6895f  Q20151582015164.L3b_R7_SCI_V4.0.main.bz2

        print(g_module_name + "query_string [" + query_string + "]")
        print("curl '" + query_string + "'");

    # Loop up to CONST_GET_FILE_CHECKSUM_MAX_ATTEMPTS in case there are error with connecting to the server.
    # This can happen if another process on the same machine is connected to the server.

    crawl_start_time = time.time();
    attempt_number = 1;
    while (attempt_number <= CONST_GET_FILE_CHECKSUM_MAX_ATTEMPTS):
        try:
           content_raw = urllib.request.urlopen(query_string).read();
           # If read successfully, break out of while loop.
           break;
        except urllib.error.URLError as e:
           if (g_debug):
               print(g_module_name + "WARN: Failed to perform query: " + query_string);
               print(g_module_name + "WARN: attempt_number " + str(attempt_number) + " CONST_GET_FILE_CHECKSUM_MAX_ATTEMPTS " + str(CONST_GET_FILE_CHECKSUM_MAX_ATTEMPTS));
               print(g_module_name + "WARN: sleep 5 seconds");
           # Go to sleep to give the other process time to finish:
           print(type(e))
           log_this("ERROR",debug_module,"CHECKSUM_GET" + " " + search_filter + " " + "SLEEPING " + str(CONST_GET_FILE_CHECKSUM_MAX_SLEEP_IN_BETWEEN_WAITS) + " " + "SECONDS");
           time.sleep(CONST_GET_FILE_CHECKSUM_MAX_SLEEP_IN_BETWEEN_WAITS); # Sleep for CONST_GET_FILE_CHECKSUM_MAX_SLEEP_IN_BETWEEN_WAITS seconds to give the process to finish.
        attempt_number += 1;

    # If we had made the max attempts allowed, return now with empty checksum.
    if (attempt_number > CONST_GET_FILE_CHECKSUM_MAX_ATTEMPTS):
        return(o_checksum_part);


    # If we got to here, everything is OK.  We can now parse through the content_raw to get our checksum.
    crawl_stop_time= time.time();

    content_as_list_unsorted = content_raw.split("\n");

    for one_line in content_as_list_unsorted:
        # Only process if the line contains regular expression.
        if (g_debug):
            print(g_module_name + "one_line [" + one_line + "]");
        if (re.search(pattern_to_look_for,one_line)):
            if (g_debug):
                print(g_module_name + "yes found pattern");

            # Parse the line into 2 tokens.

            tokens = re.findall(r'[^"\s]\S*|".+?"', one_line)

            if (len(tokens) >= 2):
                o_checksum_part = tokens[0];
                filename_part   = tokens[1];
                print(o_checksum_part);
                if (re.search(pattern_to_look_for,filename_part)):
                    new_line = getfile_uri + "/" + filename_part + " " + o_checksum_part
            else:
                if (g_debug):
                    print(g_module_name + "ERROR: Line only contain 1 token [" + one_line + "]")
        else:
            if (g_debug):
                print(g_module_name + "no found pattern");

    if (g_debug):
        print(g_module_name + "query_string [" + query_string + "]")

    return(o_checksum_part);

if __name__ == "__main__":
   #
   # Test some a MODIS file with search_dtype and search_sensor with values.
   #
   search_filter = "T2015001235000.L2_LAC_SST.nc";
   search_dtype  = "L2";
   search_sensor = "modis";
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   #
   # Test a VIIRS file with search_dtype and search_sensor with values.
   #
   search_filter = "V2016293000000.L2_SNPP_SST.nc";
   search_dtype  = "L2";
   search_sensor = "viirs";
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   #
   # Test a VIIRS file with search_dtype and search_sensor as None.
   #
   search_filter = "V2016293000000.L2_SNPP_SST.nc";
   search_dtype  = None;
   search_sensor = None; 
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   #
   # Test some Aquarius L3m files.
   #

   search_filter = "Q2015001.L3m_DAY_EVQLA_V4.1.0_scat_wind_speed_1deg.bz2";
   search_dtype  = None;
   search_sensor = None; 
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)


   search_filter = "Q2015158.L3m_DAY_SOILMA_V4.0_rad_sm_1deg.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   search_filter = "Q2015158.L3m_DAY_SOILMD_V4.0_rad_sm_1deg.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)


   search_filter = "Q2015158.L3m_DAY_SOILM_V4.0_rad_sm_1deg.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   #
   # Test some Aquarius L3b files.
   #

   search_filter = "Q2015158.L3b_DAY_SCI_V4.0.main.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)


   search_filter = "Q2015158.L3b_DAY_SOILMA_V4.0.main.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   search_filter = "Q2015158.L3b_DAY_SOILMD_V4.0.main.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   search_filter = "Q2015158.L3b_DAY_SOILM_V4.0.main.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   search_filter = "Q20151582015164.L3b_R7_EVSCI_V4.5.1.main.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   search_filter = "Q20151582015164.L3b_R7_SCI_V4.0.main.bz2";
   search_dtype  = None;
   search_sensor = None;
   o_checksum_part = get_file_checksum(search_filter,
                                       search_dtype,
                                       search_sensor);
   print("search_filter",search_filter,"o_checksum_part",o_checksum_part)

   exit(0);
