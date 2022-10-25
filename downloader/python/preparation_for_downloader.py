#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$
# DO NOT EDIT THE LINE ABOVE - IT IS AUTOMATICALLY GENERATED BY CM

# Function to prepare for downloader by:
#
#   1.  Parse a line of text for file name to download, checksum
#
#------------------------------------------------------------------------------------------------

import os
import sys

from log_this import log_this;
import settings;

from const import CONST_GETFILE_URI;

#------------------------------------------------------------------------------------------------------------------------
def preparation_for_downloader(i_one_line,
                               i_separator_character,
                               i_perform_checksum_flag,
                               i_top_level_output_directory,
                               i_processing_level,
                               i_processing_type,
                               i_filelist_name,
                               i_scratch_area,
                               i_today_date):

    debug_module = "preparation_for_downloader:";
    debug_mode   = 0;
    g_routine_name = "generic_level2_downloader";

    if (os.getenv('CRAWLER_SEARCH_DEBUG_FLAG','') == 'true'):
        debug_mode = 1;

    if (debug_mode):
        print("i_one_line",i_one_line);
        print("i_separator_character",i_separator_character);
        print("i_perform_checksum_flag",i_perform_checksum_flag);
        print("i_top_level_output_directory",i_top_level_output_directory);
        print("i_processing_level",i_processing_level);
        print("i_processing_type",i_processing_type);
        print("i_filelist_name",i_filelist_name);
        print("i_scratch_area",i_scratch_area);
        print("i_today_date",i_today_date);

    # Default http_address if the directory name does not start with http.
    # That way, if the full pathname is only file name, we can preceed it with this address for the wget command to use.

    getfile_uri          = CONST_GETFILE_URI;

    # Return variables.

    o_preparation_status  = 0; # A value of 0 means successful, 1 means failed.
    o_checksum_value      = ""; # Reset checksum value to empty string every time so we know to check for it or not.
    o_checksum_status     = 1;# A default status of 1 so that even if the checksum was not check, we have a good value for later logic.

    o_incomplete_filename                   = ""; # Set name to blank as initial value for every loop so we know if we need to remove it for testing.
    o_full_pathname_to_download             = "";
    o_final_location_of_downloaded_file     = "";
    o_temporary_location_of_downloaded_file = "";

    splitted_array = [];
    if (i_separator_character == 'COMMA'):
        splitted_array = i_one_line.split(','); 
    elif (i_separator_character == 'SPACE'):
        splitted_array = i_one_line.split(' '); 
    else:
        splitted_array = i_one_line.split(' '); 

    o_full_pathname_to_download = splitted_array[0];

    # If is to perform the checksum, get it here. 

    if (i_perform_checksum_flag == 'yes'):
        if (len(splitted_array) > 1):
            # Assume the checksum is the 2nd token.
            #    40cee09a90c453a89bddd1850666a772f69e2fcc
            #    12345678901234567890123456789012345678901
            o_checksum_value = splitted_array[1]; 

            # Don't forget to remove the carriage return otherwise the checksum comparison may fail.
            o_checksum_value.rstrip('\n');

            o_checksum_value = o_checksum_value.strip();
        # end if (len(splitted_array) > 1)
    # end if (i_perform_checksum_flag == 'yes'):

    if (debug_mode):
        print(debug_module + "i_perform_checksum_flag",i_perform_checksum_flag);
        print(debug_module + "o_full_pathname_to_download",o_full_pathname_to_download);
        print(debug_module + "o_checksum_value",o_checksum_value);

    # Remove the carriage return and any surrounding spaces.

    o_full_pathname_to_download.rstrip('\n').strip();

    # If the first column starts with L, we get the name from the next column.
    # This file format is typically sent from OBPG if they have a large amount of files they wish to distribute
    # but don't want to publish them on the "Recent" page.
    #
    #    L2,A2011001000000.L2_LAC_OC.bz2,2013-02-26 09:08:00,sha1:0ba643cf43cb778d3bdd3076b0cbe94bd099f8fc
    #    L2,A2011001000500.L2_LAC_OC.bz2,2013-02-26 09:17:00,sha1:40cee09a90c453a89bddd1850666a772f69e2fcc

#    o_full_pathname_to_download = "L";
#    i_one_line = "L2,A2011001000000.L2_LAC_OC.bz2,2013-02-26 09:08:00,sha1:0ba643cf43cb778d3bdd3076b0cbe94bd099f8fc";
    if (o_full_pathname_to_download[0] == 'L'):
        o_full_pathname_to_download = splitted_array[1];
        # The checksum will also be in the "wrong" column.  We have to get it on the alternate field.
        o_checksum_value = parse_checksum_on_alternate_field(i_one_line,"COMMA");
        o_checksum_value = o_checksum_value.rstrip('\n').strip();

    if (debug_mode):
        print(debug_module + "i_perform_checksum_flag",i_perform_checksum_flag);
        print(debug_module + "o_full_pathname_to_download",o_full_pathname_to_download);
        print(debug_module + "o_full_pathname_to_download[0]",o_full_pathname_to_download[0]);
        print(debug_module + "o_checksum_value",o_checksum_value);
    
    # Get just the filename without the pathname.

    filename_only = os.path.basename(o_full_pathname_to_download);

    # Prepend the o_full_pathname_to_download with the default address if it does not start with http.

    path_name_start_with = o_full_pathname_to_download[0:4]; # Get the first 4 characters

    if (debug_mode):
        print(debug_module + "path_name_start_with[",path_name_start_with,"]");
        print(debug_module + "filename_only[",filename_only,"]");

    if (path_name_start_with != "http"):
        o_full_pathname_to_download = getfile_uri + "/"  + filename_only; # Prepend with "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile" for the wget command.

    destination_output_directory = i_top_level_output_directory + "/" + settings.g_gdjm.get_output_sub_directory_name(i_processing_level,i_processing_type);

    if (not os.path.exists(destination_output_directory)):
        try:
            print(debug_module + "create directory" + destination_output_directory);
            os.umask(000)
            os.makedirs(destination_output_directory, 0o777)    # NET edit. (Allow removal of directory on disk)
        except OSError as exception:
            print(debug_module + "WARN: Cannot create directory" + destination_output_directory);
            # For now, we don't do anything but print a WARNING.
            o_preparation_status = 1; # A value of 0 means successful, 1 means failed.
            return (o_preparation_status,
                    o_checksum_value,
                    o_checksum_status,
                    o_incomplete_filename,
                    o_full_pathname_to_download,
                    o_temporary_location_of_downloaded_file,
                    o_final_location_of_downloaded_file);

    # Create a hidden directory for temporary download if it does not exist yet.
    hidden_download_directory = destination_output_directory + "/.hidden"; 

    if (not os.path.exists(hidden_download_directory)):
        try:
            print(debug_module + "create directory " + hidden_download_directory);
            os.umask(000)
            os.makedirs(hidden_download_directory, 0o777)    # NET edit. (Allow removal of directory on disk)
        except FileExistsError:
            print(debug_module + "INFO: Directory exists" + hidden_download_directory)
        except OSError as exception:
            print(debug_module + "WARN: Cannot create directory" + hidden_download_directory);
            # For now, we don't do anything but print a WARNING.

    # Now, we can create the final location of the downloaded file.

    o_final_location_of_downloaded_file     = destination_output_directory + "/" + filename_only;
    o_temporary_location_of_downloaded_file = hidden_download_directory    + "/" + filename_only;

    # Also remove any temporary files, we remove it so it can be download again. 
    # Perhaps we can make this a flag from command line.

    if (os.path.exists(o_temporary_location_of_downloaded_file)):
        log_this("INFO",g_routine_name,"REMOVE_EXISTING_FILE " + o_temporary_location_of_downloaded_file);
        os.remove(o_temporary_location_of_downloaded_file);

    return (o_preparation_status,
            o_checksum_value,
            o_checksum_status,
            o_incomplete_filename,
            o_full_pathname_to_download,
            o_temporary_location_of_downloaded_file,
            o_final_location_of_downloaded_file);

#------------------------------------------------------------------------------------------------------------------------
def parse_checksum_on_alternate_field(i_one_line,
                                      i_separator_character):
    # For alternate line from OBPG, it starts with "L2".
    # The checksum is the 4th field, after the sha1:
    #    L2,A2011001000000.L2_LAC_OC.bz2,2013-02-26 09:08:00,sha1:0ba643cf43cb778d3bdd3076b0cbe94bd099f8fc
    #    0              1                       2                         3


    o_checksum_value = "";

    splitted_array = [];
    if (i_separator_character == 'COMMA'):
        splitted_array = i_one_line.split(',');
    elif (i_separator_character == 'SPACE'):
        splitted_array = i_one_line.split(' ');
    else:
        splitted_array = i_one_line.split(' ');

    if (splitted_array[0][0] == 'L'):
        splitted_fourth_fields = splitted_array[3].split(':');
        o_checksum_value = splitted_fourth_fields[1];

    return(o_checksum_value);



if __name__ == "__main__":
    debug_module = "preparation_for_downloader:";
    debug_mode   = 1;

    i_one_line = "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2016008000000.L2_LAC_OC.nc d2538d481de288cb0774e24b9458c59601b2cfe4";
    i_separator_character = "SPACE";
    i_perform_checksum_flag = "no";
    i_top_level_output_directory = "/data/dev/scratch/qchau/IO/data";
    i_processing_level = "L2";
    i_processing_type = "MODIS_A";
    i_filelist_name = "/home/qchau/sandbox/trunk/ghrsst-rdac/combine/src/main/python/resources/modis_aqua_filelist.txt.daily_2016_008_date_2016_01_08";
    i_scratch_area = "/data/dev/scratch/qchau/scratch_temp2";
    i_today_date = "17_02_01_11_02_53";

    # Because we are running this as main, we have to explicitly call the init() function.
    settings.init();

    (o_preparation_status,
     o_checksum_value,
     o_checksum_status,
     o_incomplete_filename,
     o_full_pathname_to_download,
     o_temporary_location_of_downloaded_file,
     o_final_location_of_downloaded_file)    = preparation_for_downloader(i_one_line,
                                                                          i_separator_character,
                                                                          i_perform_checksum_flag,
                                                                          i_top_level_output_directory,
                                                                          i_processing_level,
                                                                          i_processing_type,
                                                                          i_filelist_name,
                                                                          i_scratch_area,
                                                                          i_today_date);




    print("i_one_line",i_one_line);
    print("i_perform_checksum_flag",i_one_line);
    print("o_preparation_status",o_preparation_status);
    print("o_checksum_value",o_checksum_value);
    print("o_checksum_status",o_checksum_status);
    print("o_incomplete_filename",o_incomplete_filename);
    print("o_full_pathname_to_download",o_full_pathname_to_download);
    print("o_temporary_location_of_downloaded_file",o_temporary_location_of_downloaded_file);
    print("o_final_location_of_downloaded_file",o_final_location_of_downloaded_file);

    # Do the 2nd run with value of i_perform_checksum_flag to "yes"

    i_perform_checksum_flag = "yes";
    (o_preparation_status,
     o_checksum_value,
     o_checksum_status,
     o_incomplete_filename,
     o_full_pathname_to_download,
     o_temporary_location_of_downloaded_file,
     o_final_location_of_downloaded_file)    = preparation_for_downloader(i_one_line,
                                                                          i_separator_character,
                                                                          i_perform_checksum_flag,
                                                                          i_top_level_output_directory,
                                                                          i_processing_level,
                                                                          i_processing_type,
                                                                          i_filelist_name,
                                                                          i_scratch_area,
                                                                          i_today_date);




    print("i_one_line",i_one_line);
    print("i_perform_checksum_flag",i_perform_checksum_flag);
    print("o_preparation_status",o_preparation_status);
    print("o_checksum_value",o_checksum_value);
    print("o_checksum_status",o_checksum_status);
    print("o_incomplete_filename",o_incomplete_filename);
    print("o_full_pathname_to_download",o_full_pathname_to_download);
    print("o_temporary_location_of_downloaded_file",o_temporary_location_of_downloaded_file);
    print("o_final_location_of_downloaded_file",o_final_location_of_downloaded_file);

    # Do the 3rd run with value of i_perform_checksum_flag to "yes" and i_one_line without the leading http


    i_perform_checksum_flag = "yes";
    i_one_line = "A2016008000000.L2_LAC_OC.nc d2538d481de288cb0774e24b9458c59601b2cfe4";

    (o_preparation_status,
     o_checksum_value,
     o_checksum_status,
     o_incomplete_filename,
     o_full_pathname_to_download,
     o_temporary_location_of_downloaded_file,
     o_final_location_of_downloaded_file)    = preparation_for_downloader(i_one_line,
                                                                          i_separator_character,
                                                                          i_perform_checksum_flag,
                                                                          i_top_level_output_directory,
                                                                          i_processing_level,
                                                                          i_processing_type,
                                                                          i_filelist_name,
                                                                          i_scratch_area,
                                                                          i_today_date);


    print("i_one_line",i_one_line);
    print("i_perform_checksum_flag",i_perform_checksum_flag);
    print("o_preparation_status",o_preparation_status);
    print("o_checksum_value",o_checksum_value);
    print("o_checksum_status",o_checksum_status);
    print("o_incomplete_filename",o_incomplete_filename);
    print("o_full_pathname_to_download",o_full_pathname_to_download);
    print("o_temporary_location_of_downloaded_file",o_temporary_location_of_downloaded_file);
    print("o_final_location_of_downloaded_file",o_final_location_of_downloaded_file);
