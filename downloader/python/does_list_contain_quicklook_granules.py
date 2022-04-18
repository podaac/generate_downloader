#!/usr/local/bin/python
#
#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$

# Given a file containing a list of URIs, program will look through all the names and returns true if it contain at least one quicklook file.
# A quicklook file is determined by the number of day different from today's date and the date of the granule.
#
# The URI is in the form of:
#
#     https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/A2013048142500.L2_LAC_OC.bz2
#
#------------------------------------------------------------------------------------------------

import datetime;
import os;
import sys;

def get_actual_date_from_year_doy(i_filename,i_year,i_doy):
    # If the name is from the new 2019 format, we extract convert the given i_doy to month and day then
    # combine with the year to return the new name for the next function to look. 
    function_name = 'get_actual_date_from_year_doy:';

    o_actual_date_to_look = str(i_year) + str(i_doy);

    new_format_flag = False;

    if 'AQUA_MODIS'  in i_filename or \
       'TERRA_MODIS' in i_filename or \
       'SNPP_VIIRS'  in i_filename:
        new_format_flag = True;

    if new_format_flag:
        # AQUA_MODIS.20191016T042500.L2.OC.nc
        # Use the given year and ay of year we build a new date, then we can get the
        # year, month and day from that.
        beginning_of_year_date = datetime.datetime(int(i_year),1,1);
        dtdelta = datetime.timedelta(days=int(i_doy)) 
        granule_date = beginning_of_year_date + dtdelta;
        month_field  = granule_date.month;
        day_field    = granule_date.day;
        year_field   = i_year;

        # Convert the year, month day to day of year
        granule_date = datetime.datetime(int(year_field),int(month_field),int(day_field));
        # Build the actual string to look.
        o_actual_date_to_look = str(i_year) + str('{:02d}'.format(month_field)) + str('{:02d}'.format(day_field));

    else: 
        pass;


    return(o_actual_date_to_look);

def does_list_contain_quicklook_granules(i_filelist_name,
                                         i_separator_character,
                                         i_year,
                                         i_month,
                                         i_day,
                                         i_yesterday_year,
                                         i_yesterday_month,
                                         i_yesterday_day):

    # Do a sanity check on the existence of the input file.
    debug_module = "does_list_contain_quicklook_granules";
    debug_mode   = 0;

    if (not os.path.isfile(i_filelist_name)):
        print("ERROR:NO_FILES_FOUND",i_filelist_name);

    with open(i_filelist_name) as f:
        list_of_files_to_inspect = f.read().splitlines()


    # Everything is OK, we can proceed with the inspecting task.

    num_files_read            = 0;
    time_spent_in_inspecting = 0; 
    total_Bytes_in_files      = 0; 
    total_Bytes_inspected    = 0; 

    # Default http_address if the directory name does not start with http.
    # That way, if the full pathname is onlya file name, we can preceed it with this address for the wget command to use.

    default_http_address = "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile";

    # For every name in the list, we check to see if it contains a "quicklook" name.

    num_sst_sst4_files = len(list_of_files_to_inspect);
    index_to_sst_sst4_list = 0;
    checksum_value = "";
    checksum_status = 1;
    num_files_inspected = 0;
    found_quicklook_flag = 0;

    while ((index_to_sst_sst4_list < num_sst_sst4_files) and (found_quicklook_flag == 0)):

        checksum_value = "";  # Reset checksum value to empty string every time so we know to check for it or not.
        checksum_status = 1;  # A default status of 1 so that even if the checksum was not check, we have a good value for later logic.

        # Get one line and split the line based on spaces.  Use the first token as the name.
        one_line = list_of_files_to_inspect[index_to_sst_sst4_list]; 

        splitted_array = [];
        if (i_separator_character == 'COMMA'):
            splitted_array = one_line.split(','); 
        elif (i_separator_character == 'SPACE'):
            splitted_array = one_line.split(' '); 
        else:
            splitted_array = one_line.split(' '); 

        full_pathname_to_inspect = splitted_array[0];

       # Remove the extra spaces and carriage returns.

        full_pathname_to_inspect = full_pathname_to_inspect.rstrip().rstrip('\n');

        # Get the initial string of the full pathname.

        full_pathname_directory_only = os.path.dirname(full_pathname_to_inspect);

        # Get just the filename without the pathname.

        filename_only = os.path.basename(full_pathname_to_inspect);

        # Prepend the full_pathname_to_inspect with the default address if it does not start with http.

        path_name_start_with = full_pathname_to_inspect[0:4]; # Get the first 4 characters

        if (path_name_start_with != "http"):
            full_pathname_to_inspect = default_http_address + "/" + filename_only; # Prepend with "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile" for the wget command.

        date_to_search      = "";
        myyearday           = "";
        correct_day_of_year = ""; 
        doy_as_string       = "";

        myyearday = convert_to_doy(i_year,i_month);
        correct_day_of_year = myyearday + int(i_day); 
        if (debug_mode):
            print("i_year",i_year,"i_month",i_month,"i_day",i_day,"correct_day_of_year",correct_day_of_year);

        # Fill the necessary leading zeros to get to 3 digits.

        doy_as_string = correct_day_of_year;
        if (correct_day_of_year < 10):
            # Add 2 leading zeros if less than 10.
            doy_as_string = "00" + str(correct_day_of_year);
        elif ((correct_day_of_year >= 10) and (correct_day_of_year <= 99)):
            # Add 2 leading zeros if between 10 and 99 (included);
            doy_as_string = "0"  + str(correct_day_of_year);

        date_to_search = str(i_year) + str(doy_as_string);

        # New logic: If we are dealing with the new name format, we must convert the day of year into month and day to match the new name.
        # For example if the filename was TERRA_MODIS.20170209T000000.L2.SST.NC we have to convert to 
        # the field date_to_search to 20170212 since day of year 42 of year 2017 is the same as February 9, 2017. 
        date_to_search = get_actual_date_from_year_doy(filename_only,i_year,doy_as_string);

        if (debug_mode):
            print("filename_only",filename_only,"date_to_search",date_to_search,"filename_only.find(date_to_search)",filename_only.find(date_to_search));
        if (filename_only.find(date_to_search) >= 0):
            found_quicklook_flag = 1;

        # Do an additional search for yesterday's date if we are behind.

        myyearday = convert_to_doy(i_yesterday_year,i_yesterday_month);
        correct_day_of_year = myyearday + int(i_yesterday_day);
        if (debug_mode):
            print("i_yesterday_year",i_yesterday_year,"i_yesterday_month",i_yesterday_month,"i_yesterday_day",i_yesterday_day,"correct_day_of_year",correct_day_of_year);

        # Fill the necessary leading zeros to get to 3 digits.

        doy_as_string = correct_day_of_year;
        if (correct_day_of_year < 10):
            # Add 2 leading zeros if less than 10.
            doy_as_string = "00" + str(correct_day_of_year);
        elif ((correct_day_of_year >= 10) and (correct_day_of_year <= 99)):
            # Add 2 leading zeros if between 10 and 99 (included);
            doy_as_string = "0"  + str(correct_day_of_year);

        date_to_search = str(i_yesterday_year) + str(doy_as_string);
        # New logic: If we are dealing with the new name format, we must convert the day of year into month and day to match the new name.
        date_to_search = get_actual_date_from_year_doy(filename_only,i_year,doy_as_string);

        if (debug_mode):
            print("filename_only",filename_only,"date_to_search",date_to_search,"filename_only.find(date_to_search)",filename_only.find(date_to_search));

        if (filename_only.find(date_to_search) >= 0):
            found_quicklook_flag = 1;


        # Do one final check to see if the name contains these tokens.
        if 'AQUA_MODIS'  in filename_only or \
           'TERRA_MODIS' in filename_only or \
           'SNPP_VIIRS'  in filename_only:
           if '.NRT' in filename_only:
                found_quicklook_flag = 1;

        # Keep track of how many files we have read, inspected and index.
        num_files_read += 1;
        num_files_inspected   += 1;
        index_to_sst_sst4_list += 1;

    # ---------- Close up shop ----------

    print("FOUND_QUICKLOOK_FLAG",found_quicklook_flag);
    return;

# End of of main subroutine does_list_contain_quicklook_granules.

#-------------------------------------------------------------------------------
def is_leap_year(the_year):
        debug_module = "is_leap_year:";
        debug_mode   = 0;
        if (debug_mode):
            print(debug_module + "the_year",the_year,"type(the_year)",type(the_year));
        #  A year is a leap year if it is
        #                       divisible by 4
        #                       but if it is divisible by 100 then it isn't
        #                       unless it is divisible by 400 also
        #
        if ((the_year % 4) == 0):
        # divisible by 4
                if ((the_year % 100) == 0):
                # divisible by 100, 4
                        if ((the_year % 400) == 0):
                                # divisible by 400, 100, and 4
                                return(1); # it is a leap year
                        else:
                        # divisible by 100, 4
                                return(0); # not a leap year
                else:
                # divisible by 4
                        return(1); # it is a leap year
        else:
        # not divisible by 4
                return(0); #not a leap year
        return(0);
#-------------------------------------------------------------------------------
def convert_to_doy(i_year,
                   i_month):
    debug_module = "convert_to_doy:";
    debug_mode   = 0;
    #
    # Function converts a month and a year to a day of year.
    #

    # Retrieve the input:


    # Define output.

    o_doy = 0;

    #
    # Check for leapness
    #

    year_is_leap = is_leap_year(int(i_year));

    days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    daysleap = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

    month_index = int(i_month) - 1;

    if (year_is_leap):
        o_doy = daysleap[month_index];
    else:
        o_doy = days[month_index];

    if (debug_mode):
        print(debug_module + "convert_to_doy:o_doy  = ",o_doy,"type(o_doy)",type(o_doy),"i_year",i_year,"i_month",i_month);
    return(o_doy);


if __name__ == "__main__":
    debug_module = "does_list_contain_quicklook_granules:";
    debug_mode   = 0;

	# python does_list_contain_quicklook_granules.py /home/qchau/scratch/viirs_level2_download_list/viirs_filelist.txt.daily_2017_073_date_2017_03_14       SPACE 2013 5 30 2013 5 29
	# python does_list_contain_quicklook_granules.py /home/qchau/scratch/viirs_level2_download_list/viirs_filelist.txt.daily_2019_305_date_2019_11_01 SPACE 2019 11 1 2019 10 31
	# python does_list_contain_quicklook_granules.py /home/qchau/scratch/modis_level2_download_list/modis_terra_filelist.txt.daily_2017_040_date_2017_02_09 SPACE 2017 2 9 2017  2 8 
	# python does_list_contain_quicklook_granules.py /home/qchau/scratch/modis_level2_download_list/modis_terra_filelist.txt.daily_2017_040_date_2017_02_09 SPACE 2017 2 10 2017 2 9 
	# python does_list_contain_quicklook_granules.py /home/qchau/scratch/modis_level2_download_list/modis_terra_filelist.txt.daily_2017_040_date_2017_02_09 SPACE 2017 2 11 2017 2 10

    i_filelist_name        = sys.argv[1];
    i_separator_character  = sys.argv[2];
    i_year                 = sys.argv[3];
    i_month                = sys.argv[4];
    i_day                  = sys.argv[5];
    i_yesterday_year       = sys.argv[6];
    i_yesterday_month      = sys.argv[7];
    i_yesterday_day        = sys.argv[8];

    if (debug_mode):
        print("1:", i_filelist_name);
        print("2:",i_separator_character);
        print("3:",i_year);
        print("4:",i_month);
        print("5:",i_day);
        print("6:",i_yesterday_year);
        print("7:",i_yesterday_month);
        print("8:",i_yesterday_day);


    does_list_contain_quicklook_granules(i_filelist_name,
                                         i_separator_character,
                                         i_year,
                                         i_month,
                                         i_day,
                                         i_yesterday_year,
                                         i_yesterday_month,
                                         i_yesterday_day);
