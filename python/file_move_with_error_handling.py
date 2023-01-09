#***************************************************************************
#
#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$
#***************************************************************************
#
# Function perform a move() on the file to be moved to destination directory name and returns 1 if successful and 0 if failed.
# A sigevent will be raised if the file cannot be moved for any reason.

import os
import shutil

from log_this import log_this;
from raise_sigevent_wrapper import raise_sigevent_wrapper;

def file_move_with_error_handling(i_filename_to_move,
                                  i_destination_name):

    debug_module = "file_move_with_error_handling:";
    debug_mode   = 1;

    o_move_status = 1;

    # Do a sanity check by checking to see if the input file exists.
    if (not os.path.isfile(i_filename_to_move)):
        o_move_status = 0;

        sigevent_type     = 'ERROR'
        sigevent_category = 'GENERATE'
        sigevent_description = "FILE_MOVE_FAILED_SOURCE_FILE_DOES_NOT_EXIST " + i_filename_to_move;
        sigevent_data        = "";
        sigevent_debug_flag  = None;

        print(debug_module + sigevent_description);

        raise_sigevent_wrapper(sigevent_type,
                               sigevent_category,
                               sigevent_description,
                               sigevent_data,
                               sigevent_debug_flag);
        return(o_move_status);

    # If the destination ends with a slash, we assume it is a directory.
    destination_is_directory_flag = 0;  # Assume the destination if not a directory first.

    last_character_of_destination = i_destination_name[len(i_destination_name)-1];

    if (last_character_of_destination == '/'):
        destination_is_directory_flag = 1;

    # If the destination is a directory and it does not exist yet, we return immediately.

    if ((destination_is_directory_flag) and (not os.path.isfile(i_destination_name))):
        o_move_status = 0;

        sigevent_type     = 'ERROR';
        sigevent_category = 'GENERATE';
        sigevent_description = "FILE_MOVE_FAILED_OUTPUT_DIRECTORY_DOES_NOT_EXIST [" + i_destination_name + "]";
        sigevent_data        = "";
        sigevent_debug_flag  = None;

        print(debug_module + sigevent_description);

        raise_sigevent_wrapper(sigevent_type,
                               sigevent_category,
                               sigevent_description,
                               sigevent_data,
                               sigevent_debug_flag);
        return(o_move_status);

    # Depends on if the destination is a directory or a file, we parse for the directory and file name.
    parent_directory_name = os.path.dirname(i_destination_name);
    name_only             = "";
    actual_destination_filename = "";

    if (destination_is_directory_flag):
        parent_directory_name = i_destination_name;           # The destination is a directory /data/dev/scratch/combiner_workspace/, then the parent directory is /data/dev/scratch/combiner_workspace/
        name_only             = os.path.basename(i_filename_to_move); # Get the file name from the source file name.
    else:
        name_only             = os.path.basename(i_destination_name); # Get the file name from the destination
    
    if (destination_is_directory_flag):
        actual_destination_filename = parent_directory_name + name_only;
    else:
        actual_destination_filename = parent_directory_name + "/" + name_only;

    # At this point, we know that the destination is either a directory or a file with an existing directory.
    # The file can then be moved.

    shutil.move(i_filename_to_move,i_destination_name);

    # Do a sanity check on the actual destination file.
    if (os.path.isfile(actual_destination_filename)):
        # Do nothing, this is good.  We have successfully moved the file.
        sigevent_msg = "FILE_MOVE_SUCCESSFUL " + i_filename_to_move + " " + actual_destination_filename;
        log_this("INFO",debug_module,sigevent_msg);
    else:
        o_move_status = 0;
        # Notify operator and return.

        sigevent_type     = 'ERROR'
        sigevent_category = 'GENERATE'
        sigevent_description = "FILE_MOVE_FAILED FROM " + i_filename_to_move + " TO " + i_destination_name;
        sigevent_data        = "";
        sigevent_debug_flag  = None;

        print(debug_module + sigevent_description);

        raise_sigevent_wrapper(sigevent_type,
                               sigevent_category,
                               sigevent_description,
                               sigevent_data,
                               sigevent_debug_flag);

    return(o_move_status);

if __name__ == "__main__":
    debug_module = "file_move_with_error_handling:";
    debug_mode   = 0;

    i_filename_to_move = "dummy_file";
    i_destination_name = "dummy_directory";

    print("touch " + i_filename_to_move);
    os.system("touch " + i_filename_to_move);
    print("mkdir " + i_destination_name);
    os.system("mkdir " + i_destination_name);

    o_move_status = file_move_with_error_handling(i_filename_to_move,
                                                  i_destination_name + "/" + i_filename_to_move);

    print("i_filename_to_move",i_filename_to_move);
    print("i_destination_name",i_destination_name);
    print("ls -l " + i_destination_name);
    os.system("ls -l " + i_destination_name);
    print("rm -f " + i_destination_name + "/" + i_filename_to_move);
    os.system("rm -f " + i_destination_name + "/" + i_filename_to_move);
    print("rmdir " + i_destination_name);
    os.system("rmdir " + i_destination_name);

    debug_module = "file_move_with_error_handling:";
    debug_mode   = 1;
