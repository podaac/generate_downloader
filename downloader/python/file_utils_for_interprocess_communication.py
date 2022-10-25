#!/usr/local/bin/perl
#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$

# File contains utilities to write/read snippet of info to file for communication between processes.

import os;
import random
import time;

#------------------------------------------------------------------------------------------------------------------------
def get_filename_for_interprocess_communication(i_scratch_area,
                                                i_processing_level,
                                                i_processing_type):

    debug_module = "get_filename_for_interprocess_communication:";
    debug_mode   = 0;

    global g_filename_for_interprocess_communication;
    g_filename_for_interprocess_communication = None;

    o_filename_for_interprocess_communication = i_scratch_area.rstrip("/") + "/" + "run_report_for_interprocess_communication_for_processing_level_" + str(i_processing_level) + "_processing_type_" + i_processing_type + "_" + str(random.randint(1, 1000)) + '.txt';
    # Update our global name if we haven't yet.
    if (g_filename_for_interprocess_communication is None):
        g_filename_for_interprocess_communication = o_filename_for_interprocess_communication;

    # Example name:
    # /home/qchau/scratch/run_report_for_interprocess_communication_for_processing_level_2_processing_type_VIIRS_1490118533.txt

    return(o_filename_for_interprocess_communication);

def delete_file_for_interprocess_communication():
    debug_module = "delete_file_for_interprocess_communication:";
    debug_mode   = 0;
    global g_filename_for_interprocess_communication;
    if (os.path.exists(g_filename_for_interprocess_communication)):
        if (debug_mode):
            print("FILE_DELETE " + g_filename_for_interprocess_communication);
        os.remove(g_filename_for_interprocess_communication);
    else:
        print(debug_module + "WARN:NO_DELETE " + g_filename_for_interprocess_communication + " FILE_NOT_EXIST");
    return;

#------------------------------------------------------------------------------------------------------------------------
def write_child_file_for_interprocess_communication(i_scratch_area,
                                                    i_processing_level,
                                                    i_processing_type,
                                                    i_message):
    # Function write a message with a carriage return to new file.

    debug_module = "write_child_file_for_interprocess_communication:";
    debug_mode   = 0;

    filename_for_interprocess_communication = get_filename_for_interprocess_communication(i_scratch_area,i_processing_level,i_processing_type);

    # It is possible to fail to open the file for writing.  We will make several attempts to open the file.

    (o_attempts_made,o_file_handle) = open_file_for_write_with_attempts(filename_for_interprocess_communication);

    # If was able to open file successfully, write the name and some metadata about processing and close file.
    if (o_file_handle is not None):
        if (debug_mode):
            print("INFO",debug_module,"WRITE_MESSAGE [" + i_message + "] to file " + filename_for_interprocess_communication);
        o_file_handle.write(i_message.rstrip("\n") + "\n");
        o_file_handle.flush();
        o_file_handle.close();
    else:
        print(debug_module,"WARN: Cannot write message [" + i_message + "] to file " + filename_for_interprocess_communication);
    return;

#------------------------------------------------------------------------------------------------------------------------
def open_file_for_write_with_attempts(i_filename_for_interprocess_communication):
    # Subroutine will attempt to open a file with a certain mode and will attempt to do so at $MAX_OPEN_ATTEMPTS times.
    # If the file does exist, it will open the file for append mode.
    # If the file does not exist, it will create the file for initial write.

    debug_module = "open_file_for_write_with_attempts:";
    debug_mode   = 0;

    o_file_handle = None;        # If this is defined when this function returned, we assume that the file open was successful.
    o_attempts_made = 0;

    # Use default 10 times and reset it to whatever environment MAX_OPEN_ATTEMPTS is set to.
    MAX_OPEN_ATTEMPTS = 1;

    attempt_index = 1;
    open_flag     = 0;

    # Loop as many times as necessary to open the file.
 
    while ((open_flag == 0) and (attempt_index <= MAX_OPEN_ATTEMPTS)):
        if (os.path.exists(i_filename_for_interprocess_communication)):
            try:
                o_file_handle = open(i_filename_for_interprocess_communication,"a");
                # Success open for appending.
                open_flag = 1;
                if (debug_mode):
                    print(debug_module + "INFO:FILE_OPENED_FOR_APPEND_FILENAME_FOR_INTERPROCESS_COMMUNICATION " + i_filename_for_interprocess_communication);
            except IOError:
                print(debug_module + "WARN:FILE_OPEN_APPEND_FAIL " + i_filename_for_interprocess_communication + " ATTEMPT_INDEX " + str(attempt_index));
        else:
            try:
                o_file_handle = open(i_filename_for_interprocess_communication,"w");
                # Success open for initial writing.
                open_flag = 1;
                if (debug_mode):
                    print(debug_module + "INFO:FILE_OPENED_FOR_OVERWRITE_FILENAME_FOR_INTERPROCESS_COMMUNICATION " + i_filename_for_interprocess_communication);
            except IOError:
                print(debug_module + "WARN:FILE_OPEN_CREATE_FAIL " + i_filename_for_interprocess_communication + " ATTEMPT_INDEX " + str(attempt_index));
        # end else part of if (os.path.exists(i_filename_for_interprocess_communication))

        # If the file is still not open yet, go to sleep for 1 second and attempt again if we have not reached the MAX_OPEN_ATTEMPTS value.
        if (open_flag == 0):
            print(debug_module + "WARN:FILE_OPEN_FOR_WRITE_FAIL " + i_filename_for_interprocess_communication + " SLEEPING_FOR_1_SECOND");
            time.sleep(1);

        attempt_index   = attempt_index + 1;
        o_attempts_made = o_attempts_made + 1;

    return(o_attempts_made,o_file_handle);

#------------------------------------------------------------------------------------------------------------------------
def open_file_for_read_with_attempts(i_filename_for_interprocess_communication):
    # Subroutine will attempt to open a file for reading will attempt to do so at MAX_OPEN_ATTEMPTS times.

    debug_module = "open_file_for_write_with_attempts:";
    debug_mode   = 0;

    o_file_handle = None;        # If this is defined when this function returned, we assume that the file open was successful.
    o_attempts_made = 0;

    # Use default 10 times and reset it to whatever environment MAX_OPEN_ATTEMPTS is set to.
    MAX_OPEN_ATTEMPTS = 1;

    attempt_index = 1;
    open_flag     = 0;

    # Loop as many times as necessary to open the file.

    while ((open_flag == 0) and (attempt_index <= MAX_OPEN_ATTEMPTS)):
        if (os.path.exists(i_filename_for_interprocess_communication)):
            try:
                o_file_handle = open(i_filename_for_interprocess_communication,"r");
                # Success open for reading.
                open_flag = 1;
                print(debug_module + "INFO:FILE_OPENED_FOR_READ_FILENAME_FOR_INTERPROCESS_COMMUNICATION " + i_filename_for_interprocess_communication);
            except IOError:
                print(debug_module + "WARN:FILE_OPEN_READ_FAIL " + i_filename_for_interprocess_communication + " ATTEMPT_INDEX " + str(attempt_index));
        # end else part of if (os.path.exists(i_filename_for_interprocess_communication))

        # If the file is still not open yet, go to sleep for 1 second and attempt again if we have not reached the MAX_OPEN_ATTEMPTS value.
        if (open_flag == 0):
            print(debug_module + "WARN:FILE_OPEN_FOR_WRITE_FAIL " + i_filename_for_interprocess_communication + " SLEEPING_FOR_1_SECOND");
            time.sleep(1);

        attempt_index   = attempt_index + 1;
        o_attempts_made = o_attempts_made + 1;

    return(o_attempts_made,o_file_handle);


#------------------------------------------------------------------------------------------------------------------------
def read_master_file_for_interprocess_communication(i_scratch_area,
                                                    i_processing_level,
                                                    i_processing_type):

    debug_module = "read_master_file_for_interprocess_communication:";
    debug_mode   = 0;
    o_run_log_array = [];

    global g_filename_for_interprocess_communication;
    print(debug_module + "g_filename_for_interprocess_communication",g_filename_for_interprocess_communication)

    try:
        with open(g_filename_for_interprocess_communication,"r") as f:
            o_run_log_array = f.read().splitlines();
        f.close();
        if (debug_mode):
            array_size = len(o_run_log_array);
            print(debug_module + "FILENAME_FOR_INTERPROCESS_COMMUNICATION " + filename_for_interprocess_communication + " ARRAY_SIZE " + str(array_size));
    except IOError:
        print("INFO",debug_module + "WARN:CANNOT_OPEN_FILE " + filename_for_interprocess_communication + " ATTEMPTS_MADE " + str(o_attempts_made));

    return (o_run_log_array);

if __name__ == '__main__':
    debug_module = 'write_child_file_for_interprocess_communication';
    debug_mode = 0;

    if (os.getenv('CRAWLER_SEARCH_DEBUG_FLAG') == 'true'):
        debug_mode   = 1;
    debug_mode = 1;

    i_scratch_area = os.getenv("HOME","") + "/" + "scratch";
    i_processing_level = 2;
    i_processing_type = "VIIRS";
    i_message         = "MY_MESSAGE";

    donotcare = write_child_file_for_interprocess_communication(i_scratch_area,
                                                                i_processing_level,
                                                                i_processing_type,
                                                                i_message);

    o_run_log_array = read_master_file_for_interprocess_communication(i_scratch_area,
                                                                      i_processing_level,
                                                                      i_processing_type);
    print("o_run_log_array",o_run_log_array);
    delete_file_for_interprocess_communication();

