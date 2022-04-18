#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$
# DO NOT EDIT THE LINE ABOVE - IT IS AUTOMATICALLY GENERATED BY CM

# This class module allows the handling of job registry/removal of the generic downloader module.

import os
import subprocess
import time
import sys

from time import gmtime, strftime;

from log_this import log_this;

from const import CONST_MAX_AGE_BEFORE_CONSIDERED_STALE, CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS;

class generic_downloader_job_manager:

    # Package private variables.

    m_crawler_info_lookup_table = {};
    m_output_directory_name_lookup_table = {};
    g_package_name = "generic_downloader_job_manager";

    #------------------------------------------------------------------------------------------------------------------------
    def __init__(self):
        # This is where we call the setup_ functions.
        self.setup_lookup_table();
        self.setup_crawler_info_lookup_table();

    #------------------------------------------------------------------------------------------------------------------------
    def setup_lookup_table(self):
        # The table consists of key = processing level combined with processing type
        #                     value = output sub directory name corresponding to processing type
        #
        # Add any new dataset(s) here.
    
       self.m_output_directory_name_lookup_table['L2_MODIS_A']  = 'MODIS_AQUA_L2_SST_OBPG';
       self.m_output_directory_name_lookup_table['L2_MODIS_T']  = 'MODIS_TERRA_L2_SST_OBPG';
       self.m_output_directory_name_lookup_table['L3_MODIS_A']  = 'MODIS_AQUA_L3_SST_OBPG';
       self.m_output_directory_name_lookup_table['L3_MODIS_T']  = 'MODIS_TERRA_L3_SST_OBPG';
       self.m_output_directory_name_lookup_table['L2_VIIRS']    = 'VIIRS_L2_SST_OBPG';
       self.m_output_directory_name_lookup_table['L2_AQUARIUS'] = 'AQUARIUS_L2_OBPG';
       self.m_output_directory_name_lookup_table['L3_AQUARIUS'] = 'AQUARIUS_L3_OBPG';
       self.m_output_directory_name_lookup_table['L4_AQUARIUS'] = 'AQUARIUS_L4_OBPG';
    
       return(1);

    #------------------------------------------------------------------------------------------------------------------------
    def setup_crawler_info_lookup_table(self):
       # The table consists of key = processing type
       #                     value = first character of granule name.
       #
       # Add any new dataset(s) here.

       self.m_crawler_info_lookup_table['MODIS_A']  = "A";
       self.m_crawler_info_lookup_table['MODIS_T']  = "T";
       self.m_crawler_info_lookup_table['VIIRS']    = "V";
       self.m_crawler_info_lookup_table['AQUARIUS'] = "Q";

       return(1);

    #------------------------------------------------------------------------------------------------------------------------
    def get_output_sub_directory_name(self,
                                      i_processing_level,
                                      i_processing_type):

         # Do a sanity check to see if the value of i_processing_type is in the m_output_directory_name_lookup_table otherwise
         # Python will crash with
         #
         #        KeyError: 

         lookup_key = i_processing_level + "_" + i_processing_type;  # The look up key is a concatenation of the processing level with the processing type.

         if (lookup_key not in self.m_output_directory_name_lookup_table):
             print(self.g_package_name + ":ERROR: Value of lookup_key " + lookup_key + " is not in m_output_directory_name_lookup_table.  Program terminating.");
             sys.exit(1);

         return(self.m_output_directory_name_lookup_table[i_processing_level + "_" + i_processing_type]);


    #------------------------------------------------------------------------------------------------------------------------
    def get_registry_sub_directory_name(self,
                                        i_processing_level,
                                        i_processing_type):

        o_registry_sub_directory = self.get_output_sub_directory_name(i_processing_level,i_processing_type) + "/.registry";
        return(o_registry_sub_directory); 

    #------------------------------------------------------------------------------------------------------------------------
    def get_download_sub_directory_name(self,
                                        i_processing_level,
                                        i_processing_type):

        o_download_sub_directory = self.get_output_sub_directory_name(i_processing_level,i_processing_type) + "/.hidden";
        return(o_download_sub_directory);

    #------------------------------------------------------------------------------------------------------------------------
    # Function register a given job by creating an empty file in the .registry directory.  This allow us later to check to see that the file
    # is deleted when it is finished.

    def register_this_job(self,
                          i_one_line,
                          i_top_level_output_directory,
                          i_processing_level,
                          i_processing_type):

        debug_module = "register_this_job:";
        debug_mode   = 1;

        o_register_status = 1;
        o_temporary_location_of_downloaded_file = "";

        i_one_line.rstrip('\n');
        splitted_tokens = i_one_line.split(' ');
        # Do a sanity check to make sure we have at least 1 token.
        if (len(splitted_tokens) < 1):
            log_this("WARN",debug_module,"Expecting at least 1 token from splitting with space i_one_line [" + i_one_line + "]");

        # It is possible that the checksum is not provided, we just print warning.
        checksum_value = "";
        if (len(splitted_tokens) < 2):
            log_this("WARN",debug_module,"Expecting at least 2 tokens from splitting with space i_one_line [" + i_one_line + "]");
        else:
            checksum_value = splitted_tokens[1]; # Retrieve the checksum so we can write it to the registry.

        saved_first_token = splitted_tokens[0];
        splitted_tokens = splitted_tokens[0].split('/');
        # Do a sanity check to make sure we have at least 1 token.
        if (len(splitted_tokens) < 1):
            log_this("WARN",debug_module,"Expecting at least 1 token from splitting with / splitted_token[0] ["  + saved_first_token + "]");

        filename_only = os.path.basename(splitted_tokens[len(splitted_tokens)-1]);  # Get the actual file name without the http and directory

        #  Get the sub directory of where the output directory lives.
        destination_output_directory = "";
        output_sub_directory_name = self.get_output_sub_directory_name(i_processing_level,i_processing_type);
        if (output_sub_directory_name != ""):
            destination_output_directory = i_top_level_output_directory + "/" + output_sub_directory_name;
        else:
            print(debug_module + "ERROR:The sub directory for processing_type " + i_processing_type + "is not supported yet.");
            exit(0);

        # Because it is possible for the directory to not exist yet, we create it here.
        if not os.path.isdir(destination_output_directory):
            print(debug_module + "mkdir " + destination_output_directory);
            # subprocess.call(["mkdir",destination_output_directory]);
            os.umask(000)
            os.mkdir(destination_output_directory, 0o777)    # NET edit. (Allow removal of directory on disk)

        hidden_download_directory = destination_output_directory + "/.registry";

        # Create directory if it does not exist yet
        if not os.path.isdir(hidden_download_directory):
            print(debug_module + "mkdir " + hidden_download_directory);
            # subprocess.call(["mkdir",hidden_download_directory]);
            os.umask(000)
            os.mkdir(hidden_download_directory, 0o777)    # NET edit. (Allow removal of directory on disk)


        o_location_of_registry_file = hidden_download_directory + "/" + filename_only;

        # Create a small file with the checksum to register this job.
        with open(o_location_of_registry_file,'w') as f:
            f.write(checksum_value + "\n");
            f.close();

        log_this("INFO",self.g_package_name,"REGISTRY_ADDED   " + o_location_of_registry_file);

        return(o_register_status,o_temporary_location_of_downloaded_file);

    #------------------------------------------------------------------------------------------------------------------------
    # Function remove a given job by deleting the registy from the .registry directory.

    def remove_this_job(self,
                        i_one_line,
                        i_top_level_output_directory,
                        i_processing_level,
                        i_processing_type):

        debug_module = "remove_this_job:";
        debug_mode   = 1;

        o_register_status = 1;
        o_location_of_registry_file = "";

        i_one_line.rstrip('\n');
        splitted_tokens = i_one_line.split(' ');
        # Do a sanity check to make sure we have at least 1 token.
        if (len(splitted_tokens) < 1):
            log_this("WARN",debug_module,"Expecting at least 1 token from splitting with space i_one_line [" + i_one_line + "]");

        # It is possible that the checksum is not provided, we just print a warning.
        checksum_value = "";
        if (len(splitted_tokens) < 2):
            log_this("WARN",debug_module,"Expecting at least 2 tokens from splitting with space i_one_line [" + i_one_line + "]");
        else:
            checksum_value = splitted_tokens[1]; # Retrieve the checksum value.

        saved_first_token = splitted_tokens[0];
        splitted_tokens = splitted_tokens[0].split('/');
        # Do a sanity check to make sure we have at least 1 token.
        if (len(splitted_tokens) < 1):
            log_this("WARN",debug_module,"Expecting at least 1 token from splitting with / splitted_token[0] ["  + saved_first_token + "]");

        filename_only = os.path.basename(splitted_tokens[len(splitted_tokens)-1]);  # Get the actual file name without the http and directory

        # Determine the output directory based on the initial character of the filename.

        first_character = filename_only[0];

        destination_output_directory = "";
        output_sub_directory_name = self.get_output_sub_directory_name(i_processing_level,i_processing_type);
        if (output_sub_directory_name != ""):
            destination_output_directory = i_top_level_output_directory + "/" + output_sub_directory_name;
        else:
            print(debug_module + "The sub directory for processing_type " + i_processing_type + "is not supported yet.");
            exit(0);

        hidden_download_directory = destination_output_directory + "/.registry";

        # Create directory if it does not exist yet
        if not os.path.isdir(hidden_download_directory):
            # subprocess.call(["mkdir",hidden_download_directory]);
            os.umask(000)
            os.mkdir(hidden_download_directory, 0o777)    # NET edit. (Allow removal of directory on disk)

        o_location_of_registry_file = hidden_download_directory + "/" + filename_only;
        if (os.path.isfile(o_location_of_registry_file)):
            os.remove(o_location_of_registry_file);
            log_this("INFO",self.g_package_name,"REGISTRY_REMOVED " + o_location_of_registry_file);
        else:
            log_this("WARN",self.g_package_name,"REGISTRY_NOT_EXIST " + o_location_of_registry_file);

        return(o_register_status,o_location_of_registry_file);

    #------------------------------------------------------------------------------------------------------------------------
    # Function check to see if a particular download job associated with a file is completed.  The completion of the job
    # is signified by the non-existence of a file or files in the .hidden directory that matches our given file name.
    # If the file exist, we also check to see how old it is.  If it older than a certain a number of seconds, we assume the download has went stale.

    def is_this_job_complete(self,
                             i_one_line,
                             i_top_level_output_directory,
                             i_processing_level,
                             i_processing_type):

        debug_module = "is_this_job_complete:";
        debug_mode   = 0;

        # The three possible states for a job are:
        #
        #   FILE_STATE_COMPLETED              The file in .hidden/.registry directory has been removed.  The download is considered done.
        #   FILE_STATE_STALE                  The file in .hidden/.registry directory is there and its age is outside the threshold window.
        #   FILE_STATE_CURRENTLY_DOWNLOADING  The file in .hidden/.registry directory is there and its age is within  the threshold window.

        o_job_is_completed_flag = 0;
        o_incomplete_job_name = "";

        splitted_tokens = i_one_line.split(' ');
        # Do a sanity check to make sure we have at least 1 token.
        if (len(splitted_tokens) < 1):
            log_this("WARN",debug_module,"Expecting at least 1 token from splitting with space i_one_line [" + i_one_line + "]");

        splitted_tokens = splitted_tokens[0].split('/');
        # Do a sanity check to make sure we have at least 1 token.
        if (len(splitted_tokens) < 1):
            log_this("WARN",debug_module,"Expecting at least 1 token from splitting with / splitted_token[0] [" + splitted_tokens[0] + "]");

        filename_only = os.path.basename(splitted_tokens[len(splitted_tokens)-1]);  # Get the actual file name without the http and directory

        #  Get the sub directory of where the output directory lives.
        destination_output_directory = "";
        output_sub_directory_name = self.get_output_sub_directory_name(i_processing_level,i_processing_type);

        if (output_sub_directory_name != ""):
            destination_output_directory = i_top_level_output_directory + "/" + output_sub_directory_name;
        else:
            print(debug_module + "ERROR:The sub directory for processing_type " + i_processing_type + " is not supported yet.");
            exit(0);

        temporary_location_of_downloaded_file = "";

        o_hidden_download_directory = destination_output_directory + "/.hidden";

        temporary_location_of_downloaded_file = o_hidden_download_directory + "/" + filename_only;
        o_lock_filename_filter = temporary_location_of_downloaded_file + ".lck.NFSLock";

        # Define the registry location so we can also look for it.
        registry_download_directory = destination_output_directory + "/.registry";
        job_registry_filename       = registry_download_directory  + "/" + filename_only;

        # Look for any file in the directory that matches our given file name.
        if (os.path.isfile(temporary_location_of_downloaded_file)):
            o_incomplete_job_name = temporary_location_of_downloaded_file;
       
        if (os.path.isfile(o_lock_filename_filter)):
            o_incomplete_job_name = o_lock_filename_filter;

        if (os.path.isfile(job_registry_filename)):
            o_incomplete_job_name = job_registry_filename;

        if (debug_mode):
           print(debug_module + "temporary_location_of_downloaded_file ",temporary_location_of_downloaded_file);
           print(debug_module + "o_lock_filename_filter                ",o_lock_filename_filter);
           print(debug_module + "o_incomplete_job_name                 ",o_incomplete_job_name);

        # If at least one file exist, we check to see how old it is.
        if ((o_incomplete_job_name != "") and (os.path.isfile(o_incomplete_job_name))):
            # We use the last access time instead of modify time because the modify time gives false time.
            #
            # An example:
            #
            # /data/dev/scratch/qchau/IO/data/MODIS_AQUA_L2_SST_OBPG/.hidden/A2015039000500.L2_LAC_OC.nc file_age_in_seconds 30592969 CONST_MAX_AGE_BEFORE_CONSIDERED_STALE 1
            #
            # current_timestamp 1465322761  Tue, 07 Jun 2016 18:06:01 GMT 
            # epoch_timestamp   1434729792  Fri, 19 Jun 2015 16:03:12 GMTk
            #
            # Clearly, the last modified time of a file we know is current is not from 2015, a year a go.

            epoch_timestamp   = os.stat(o_incomplete_job_name).st_mtime;
            current_timestamp = time.time();
            file_age_in_seconds = current_timestamp - epoch_timestamp;
            # If the file is older than a threshold, we consider it to be incomplete because the download should have been done.
            if (file_age_in_seconds > CONST_MAX_AGE_BEFORE_CONSIDERED_STALE):
                o_job_is_completed_flag = "FILE_STATE_STALE";
            else:
                o_job_is_completed_flag = "FILE_STATE_CURRENTLY_DOWNLOADING";

            if (debug_mode):
                print(debug_module + "o_incomplete_job_name " + o_incomplete_job_name + " file_age_in_seconds " + str(file_age_in_seconds) + " CONST_MAX_AGE_BEFORE_CONSIDERED_STALE " + str(CONST_MAX_AGE_BEFORE_CONSIDERED_STALE)); 
            
        else:
            o_job_is_completed_flag = "FILE_STATE_COMPLETED";

        if (debug_mode):
            print(debug_module + "i_one_line [" + i_one_line + "] o_job_is_completed_flag " + o_job_is_completed_flag + "o_incomplete_job_name [" + o_incomplete_job_name + "] o_hidden_download_directory " + o_hidden_download_directory); 

        return (o_job_is_completed_flag,o_incomplete_job_name,o_hidden_download_directory,o_lock_filename_filter);

    #------------------------------------------------------------------------------------------------------------------------
    # Given all the jobs dispatched, this function will monitor all jobs until their completion or until the threshold has reached.
    # The dispatched jobs are assumed to have been written by the sub process to the child_ledger.  The master_ledger is the other end
    # of the pipe and can now be read for each jobs dispatched.

    def monitor_job_completion(self,i_num_jobs,
                               i_top_level_output_directory,
                               i_processing_level,
                               i_processing_type,
                               i_all_of_lines_to_download):

        debug_module = "monitor_job_completion:";
        debug_mode   = 0;

        # Set the default wait in between each check and how long to wait total before considering a job had failed.

        MAX_RUNNING_THRESHOLD_IN_SECONDS  = int(i_num_jobs) * CONST_MAX_AGE_BEFORE_CONSIDERED_STALE;  # Max 5 minutes of running time is allowed per download of a file.

        # Create an array of all 0's to keep track of the job completion.  Each job will be set to 1 if it has completed.

        job_completion_flag_array = [0] * i_num_jobs;

        o_total_seconds_waited   = 0;  # How long have we waited checking for completion of the jobs.
        o_all_jobs_are_completed = 0;  # Flag to indicate if all the jobs are done.
        o_num_incompleted_jobs   = 0;  # Number of jobs not completed.
        o_hidden_download_directory = "";
        o_lock_filename_filter      = "";

        # Do a sanity check on the content of i_all_of_lines_to_download as it can be null, empty, or contains less than i_num_jobs
        if (i_all_of_lines_to_download is None) or (len(i_all_of_lines_to_download) < i_num_jobs):
            log_this("WARN",debug_module,"List i_all_of_lines_to_download is either None or len(i_all_of_lines_to_download) " + str(len(i_all_of_lines_to_download)) + " is less than i_num_jobs " + str(i_num_jobs));
            o_all_jobs_are_completed = 1;  # Set this flag to 1 even though we have an issue with either the list is empty or len is less than i_num_jobs.
            return(o_all_jobs_are_completed,
                   o_num_incompleted_jobs,
                   o_hidden_download_directory,
                   o_total_seconds_waited);

        log_this("INFO",debug_module,"MONITOR_JOB_COMPLETION START NUM_JOBS " + str(i_num_jobs) + " TOP_LEVEL_OUTPUT_DIRECTORY " + str(i_top_level_output_directory) + " PROCESSING_TYPE " + i_processing_type);
        log_this("INFO",debug_module,"MONITOR_JOB_COMPLETION NUM_BATCH_OF_LINES_TO_DOWNLOAD " + str(len(i_all_of_lines_to_download)));

        # Loop until o_all_jobs_are_completed is 1 or have waited beyond the maximum time to wait.
        iteration_number = 0;

        while ((not o_all_jobs_are_completed) and (o_total_seconds_waited <= MAX_RUNNING_THRESHOLD_IN_SECONDS)):
            iteration_number += 1;

            # For each job, we check to see if it is still running.
            # If it does, we set the flag to 1.  The flag o_all_jobs_are_completed is set to True when all the values are 1.

            o_num_incompleted_jobs   = 0;  # Number of jobs not completed.
            job_is_completed_flag = 0;
            incomplete_job_name   = "";
            num_jobs_checked      = 0;
            num_jobs_completed    = 0;

            for job_completion_index in range (i_num_jobs):
               one_line = i_all_of_lines_to_download[job_completion_index];
               one_line = one_line.rstrip('\n');

               if (debug_mode):
                   print("--------------------------------------------------------------------------------");
                   print("iteration [",iteration_number);
                   print("i_num_jobs [",i_num_jobs);
                   print("job_completion_index ",job_completion_index);
                   print("len(i_all_of_lines_to_download) ",len(i_all_of_lines_to_download));
                   print("one_line [",one_line,"]");
                   print("i_top_level_output_directory ",i_top_level_output_directory);
                   print("i_processing_type ",i_processing_type);

               # For each incomplete job, check to see if it is completed.
               if (job_completion_flag_array[job_completion_index] == 0):
                   num_jobs_checked = num_jobs_checked + 1;
                   (job_is_completed_flag,incomplete_job_name,o_hidden_download_directory,o_lock_filename_filter) = self.is_this_job_complete(one_line,
                                                                                                                                              i_top_level_output_directory,
                                                                                                                                              i_processing_level,
                                                                                                                                              i_processing_type);
                   if (debug_mode):
                        print(debug_module,iteration_number,job_completion_index,"o_lock_filename_filter",o_lock_filename_filter,"job_is_completed_flag",job_is_completed_flag);
                   if (job_is_completed_flag == "FILE_STATE_COMPLETED"):
                       job_completion_flag_array[job_completion_index] = 1;  # The job has completed, we are done with this job check.
                       num_jobs_completed = num_jobs_completed +1;
                   else:
                       o_num_incompleted_jobs = o_num_incompleted_jobs + 1;  # Keep track of the number of jobs not completed.  Its status may be the latter two of {FILE_STATE_COMPLETED,FILE_STATE_STALE,FILE_STATE_CURRENTLY_DOWNLOADING}

                   if (debug_mode):
                       print(debug_module,"ITERATION_NUMBER",iteration_number,"JOB_COMPLETION_INDEX",job_completion_index,"job_is_completed_flag",job_is_completed_flag,"[",one_line,"]");

               else: # end if ($job_completion_flag_array[$job_completion_index] == 0)
                   # This job is completed, we don't need to check for it again.
                   if (debug_mode):
                        print(debug_module,"ITERATION_NUMBER",iteration_number,"JOB_COMPLETION_INDEX",job_completion_index,"job_is_completed_flag FILE_STATE_COMPLETED","[",one_line,"]");

                   num_jobs_completed = num_jobs_completed +1;
               # end else of if (job_completion_flag_array[job_completion_index] == 0):
            # end for job_completion_index in range (i_num_jobs):

            if (debug_mode):
                print(debug_module,"ITERATION_NUMBER",iteration_number,"NUM_JOBS_CHECKED",num_jobs_checked,"OUT_OF",i_num_jobs,"NUM_JOBS_COMPLETED",num_jobs_completed,"NUM_JOBS_INCOMPLETE",o_num_incompleted_jobs);

            # Here we temporary assume that all the jobs are completed.
            # If any of the flag is zero, we flip it back to false.  Basically, we only consider all jobs are done if each individual element
            # in array job_completion_flag_array is set to 1.

            optimistic_flag_jobs_done = 1;

            jobs_not_completed = "";

            # Collect the names of all the jobs not completed yet so the operator knows.
            found_first_incomplete_job_flag = 0;
            for job_completion_index in range (i_num_jobs):
                if (job_completion_flag_array[job_completion_index] == 0):
                    optimistic_flag_jobs_done = 0;
                    # Collect the names of all the jobs not completed so the operator knows.
                    if (found_first_incomplete_job_flag == 0):
                        jobs_not_completed = str("{0:d}".format(job_completion_index+1));  # If this is the first name, don't add the leading space.
                        found_first_incomplete_job_flag = 1;  # Now that we have found the first incomplete job, we set this flag to 1.
                    else:
                        jobs_not_completed = jobs_not_completed + " " + str("{0:d}".format(job_completion_index+1));
                # end if (job_completion_flag_array[job_completion_index] == 0)
            # end for job_completion_index in range (i_num_jobs):

            # Print this for every loop so the operator will know how many jobs are not complete for every iteration.
            now_is = strftime("%a %b %d %H:%M:%S %Y",gmtime());
            if (debug_mode):
                print(debug_module + "[" + now_is + "]:" + "iteration_number ",iteration_number,"o_total_seconds_waited",o_total_seconds_waited,"CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS",CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS,"MAX_RUNNING_THRESHOLD_IN_SECONDS",MAX_RUNNING_THRESHOLD_IN_SECONDS," jobs_not_completed = [",jobs_not_completed,"]");

            # Now, we do a final check to see if all the flags are true via the one flag optimistic_flag_jobs_done
            # Otherwise, we wait CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS seconds and check again.

            if (optimistic_flag_jobs_done == 1):
                # All the jobs are done, we can now exit this forever loop.
                o_all_jobs_are_completed = 1;
            else:
                if (CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS > 0):
                    if (debug_mode):
                        print(debug_module,"SLEEPING_FOR CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS",CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS);
                    # The job is not done yet, we wait again.
                    time.sleep(CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS);

                # Keep track of how long we have waited so the loop can stop when this value greater than MAX_RUNNING_THRESHOLD_IN_SECONDS.
                o_total_seconds_waited = o_total_seconds_waited + CONST_MAX_WAIT_DURATION_BETWEEN_CHECKS;
            # end else if ($optimistic_flag_jobs_done == 1)
        # end while ((not o_all_jobs_are_completed) and (o_total_seconds_waited <= MAX_RUNNING_THRESHOLD_IN_SECONDS)):

        if (o_all_jobs_are_completed):
            log_this("INFO",debug_module,"MONITOR_JOB_COMPLETION STOP  NUM_JOBS " + str(i_num_jobs) + " TOP_LEVEL_OUTPUT_DIRECTORY " + i_top_level_output_directory + " PROCESSING_TYPE " + i_processing_type + " JOBS_COMPLETED_FLAG " + str(o_all_jobs_are_completed) + " TOTAL_SECONDS_WAITED " + str(o_total_seconds_waited) + " MAX_RUNNING_THRESHOLD_IN_SECONDS " + str(MAX_RUNNING_THRESHOLD_IN_SECONDS));
        else:
            log_this("ERROR",debug_module,"MONITOR_JOB_COMPLETION STOP  NUM_JOBS " + str(i_num_jobs) + " TOP_LEVEL_OUTPUT_DIRECTORY " + i_top_level_output_directory + " PROCESSING_TYPE " + i_processing_type + " JOBS_COMPLETED_FLAG " + str(o_all_jobs_are_completed) + " TOTAL_SECONDS_WAITED " + str(o_total_seconds_waited) + " MAX_RUNNING_THRESHOLD_IN_SECONDS " + str(MAX_RUNNING_THRESHOLD_IN_SECONDS));

        if (debug_mode):
            print(debug_module + "in range (i_num_jobs)",list(range(i_num_jobs)));
            print(debug_module + "o_all_jobs_are_completed",o_all_jobs_are_completed);
            print(debug_module + "o_num_incompleted_jobs",o_num_incompleted_jobs);
            print(debug_module + "o_hidden_download_directory",o_hidden_download_directory);
            print(debug_module + "o_total_seconds_waited",o_total_seconds_waited);

        return(o_all_jobs_are_completed,
               o_num_incompleted_jobs,
               o_hidden_download_directory,
               o_total_seconds_waited);
