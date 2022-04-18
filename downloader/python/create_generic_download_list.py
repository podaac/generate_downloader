#***************************************************************************
#
# Copyright 2016, by the California Institute of Technology. ALL
# RIGHTS RESERVED. United States Government Sponsorship acknowledged.
# Any commercial use must be negotiated with the Office of Technology
# Transfer at the California Institute of Technology.
#
# @version $Id$
#
#****************************************************************************
#
# Python script to create a downlist of products from OBPG search resource http://oceandata.sci.gsfc.nasa.gov/search/file_search.cgi.
# This download list can then be fed to the downloader for the combine module.
#
# The format of the download list is:
#
#     filename sha1_checksum_value
#
# with space as separator.
#
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001000000.L2_SNPP_OC.nc 466cbc5de5c8286a9723eb3c935c20aa98eabbc0
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001000000.L2_SNPP_SST.nc 9f986a23eda7263aeb4c11dfb341c886ab380b5a
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001000600.L2_SNPP_OC.nc e2363a1c3db1802ac5222492210bda8f8a6ae7d3
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001000600.L2_SNPP_SST.nc afcd563a7b78731b1fce8e799a439af8a4fc5785
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001001200.L2_SNPP_OC.nc c7cbd092f179c6be700d11a32a868a9e1d3d3d61
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001001200.L2_SNPP_SST3.nc cfd5c9390e5a178acc425de95fe2e85e714dc6a2
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001001200.L2_SNPP_SST.nc 6ccd833d8453661a4e88968d61bdd33f359a691b
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001001800.L2_SNPP_OC.nc 28e24bb477934b4f79dc147f7c35c3ecb3422136
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001001800.L2_SNPP_SST3.nc e6ababb664a428916c47aa4c135045231fbd7c07
# http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001001800.L2_SNPP_SST.nc d3d2cf1763e29111782dc04cc1d10ca32ad07108
#
# Some notes about availability of VIIRS data:
#
#   http://oceandata.sci.gsfc.nasa.gov/VIIRS/L2/
#   http://oceandata.sci.gsfc.nasa.gov/VIIRS/L2/2012
#
#   The VIIRS first data is available from 2012, day 002, and 353
#   up to current time.

import datetime
import getopt
import os
import re
import requests
import sys
import time
#import urllib.request, urllib.error, urllib.parse

from generic_split_search_dates_into_months import generic_split_search_dates_into_months;

# Make a query to OBPG to fetch a list of filename and checksum.
#
# The format of the content returned from the query if the checksum was requested look like this.
# 
# 466cbc5de5c8286a9723eb3c935c20aa98eabbc0 V2016001000000.L2_SNPP_OC.nc
# 9f986a23eda7263aeb4c11dfb341c886ab380b5a V2016001000000.L2_SNPP_SST.nc
#
# Because the format of the output download list is the opposite, we have to swap the 2 columns.
#
# The following execution results in:
#
#  Getting VIIRS:
#
#   Just getting files that are current
#
#     % python create_generic_download_list.py -l L2 -t "V20*.nc" -n viirs  -d 0 -f 1 -a 1 -c 1 -g daily -b crawl_current -i output.txt -z 2
#
# Some notes:
#     1.  For some strange reason the -t "V*.nc" results in zero file found.  We need to add the first 2 digits of the year to become:
#              -t "V20*.nc"
#     2.  For the "-c crawl_current", this Python script will build the -s and -e parameters dynamically.

#  Getting the first day the data is available
#
#     % python create_generic_download_list.py -l L2 -t "V20*.nc" -n viirs  -d 0 -f 1 -a 1 -c 1 -s "2012-01-02" -e "2012-01-02" -g daily 
#
#   Just SST toward the end of the day using filter and -s -e parameters:
#
#     % python create_generic_download_list.py -l L2 -t "V2016001000000*SST.nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2016-01-01" -e "2016-01-01" -g daily
#     % python create_generic_download_list.py -l L2 -t "V2015001235*SST.nc"    -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g daily
#
#   Just SST3 files toward the end of the day using filter and -s -e parameters:
#
#     % python create_generic_download_list.py -l L2 -t "V2015001235*SST3.nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g daily
#
#   Just Ocean Color files toward the end of the day using filter and -s -e parameters:
#
#     % python create_generic_download_list.py -l L2 -t "V2015001235*OC.nc"   -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g daily
#
#   A whole day using filter (the name):
#
#     % python create_generic_download_list.py -l L2 -t "V2015001*_SNPP_*.nc"   -n viirs -d 0 -f 1 -a 1 -c 1 -g daily
#
#   A whole day using -s and -e parameters (name can be general for 2015):
#
#     % python create_generic_download_list.py -l L2 -t "V2015*_SNPP_*.nc"      -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g daily
#
#   Getting all files for a whole day using filter (the name):
#
#     % python create_generic_download_list.py -l L2 -t "V2015001*_SNPP*.nc"  -n viirs -d 0 -f 1 -a 1 -c 1 -g daily
#
#   Getting all files for a whole day using -s and -e parameters (name can be general for 2015 year):
#
#     % python create_generic_download_list.py -l L2 -t "V2015*_SNPP*.nc"     -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g daily
#
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V2015001235*SST.nc"  -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g hourly
#
# Results in 2 files:
#
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/T2015001235000.L2_SNPP_SST.nc 63a5273e0220da71cdc91a9753907eb47d03ac83
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/T2015001235500.L2_SNPP_SST.nc b99acbb3b5f3996d860c93f07cde3632df7b9617
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V201500122*SST.nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g hourly
#
# Results in 10 files:
#
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220000.L2_SNPP_SST.nc 6ca5032e549ffcdf13bf77880a7bd8852dc15ea2
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220600.L2_SNPP_SST.nc 03af00d41cbe88b26a2b0e25fca2e5de3ceb5304
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221200.L2_SNPP_SST.nc aa9ae154a597ec6491fcf55507a8d6d39305d2a9
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221800.L2_SNPP_SST.nc ca629e1f570cb1b87b8f5c8ac2972cb96678a844
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001222400.L2_SNPP_SST.nc 22006616dc70ca52640e2b66481fb62bc027b08a
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223000.L2_SNPP_SST.nc 6f55cd55f8dae702a988b4260ba37ba5a1828c14
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223600.L2_SNPP_SST.nc 5fe0bbdd42f9cbbdf7b703554994083876394294
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224200.L2_SNPP_SST.nc c5b755a607c72ccec0982860bdf44b59ceebfe9d
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224800.L2_SNPP_SST.nc 939bfb62f2aad9853e5ed98be6f8554738466744
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001225400.L2_SNPP_SST.nc 4302c478b95fa0c3219f062d2130b4fa2f6fad8c
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V201500122*SST3.nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g hourly
#
# Result in 4 files:
#
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220000.L2_SNPP_SST3.nc 426795377132c7158fc7149ec9c9f17dcb13dab4
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220600.L2_SNPP_SST3.nc 5c1520e571a0e9219c6332c8b398caa47e2ec625
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221200.L2_SNPP_SST3.nc 507f3d72b325c6c1f0425915fef364fa29e9b571
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221800.L2_SNPP_SST3.nc 9ce17bc8072ca14092bb16e76b4e4c975338b5a9
#
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V201500122*OC.nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g hourly
#
# Result in 8 files:
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221200.L2_SNPP_OC.nc 218fbc57af77b7636470106f23873289d444bb76
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221800.L2_SNPP_OC.nc d518d02fa62eeff61125e5752afd15d5142814f2
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001222400.L2_SNPP_OC.nc 1b06124c201a3edc6f93d832f51584d77cf68df6
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223000.L2_SNPP_OC.nc b9bc8a679bfb22b5c58bac18aa97033795c38073
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223600.L2_SNPP_OC.nc b46d1758844a6f994fce1350237786ec6d17dc8a
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224200.L2_SNPP_OC.nc ab028c70d33319a686d578698b6dee6d0594c8c6
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224800.L2_SNPP_OC.nc 3eeadccd67e50239dd6761096e1a1298c4c3b10a
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001225400.L2_SNPP_OC.nc 2bce3922e625da88320ba77029dfb5aa1b69c1c6
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V201500122*.nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g hourly
#
# Result in 22 files (include OC, SST, SST3):
#
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220000.L2_SNPP_SST3.nc 426795377132c7158fc7149ec9c9f17dcb13dab4
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220000.L2_SNPP_SST.nc 6ca5032e549ffcdf13bf77880a7bd8852dc15ea2
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220600.L2_SNPP_SST3.nc 5c1520e571a0e9219c6332c8b398caa47e2ec625
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001220600.L2_SNPP_SST.nc 03af00d41cbe88b26a2b0e25fca2e5de3ceb5304
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221200.L2_SNPP_OC.nc 218fbc57af77b7636470106f23873289d444bb76
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221200.L2_SNPP_SST3.nc 507f3d72b325c6c1f0425915fef364fa29e9b571
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221200.L2_SNPP_SST.nc aa9ae154a597ec6491fcf55507a8d6d39305d2a9
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221800.L2_SNPP_OC.nc d518d02fa62eeff61125e5752afd15d5142814f2
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221800.L2_SNPP_SST3.nc 9ce17bc8072ca14092bb16e76b4e4c975338b5a9
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001221800.L2_SNPP_SST.nc ca629e1f570cb1b87b8f5c8ac2972cb96678a844
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001222400.L2_SNPP_OC.nc 1b06124c201a3edc6f93d832f51584d77cf68df6
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001222400.L2_SNPP_SST.nc 22006616dc70ca52640e2b66481fb62bc027b08a
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223000.L2_SNPP_OC.nc b9bc8a679bfb22b5c58bac18aa97033795c38073
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223000.L2_SNPP_SST.nc 6f55cd55f8dae702a988b4260ba37ba5a1828c14
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223600.L2_SNPP_OC.nc b46d1758844a6f994fce1350237786ec6d17dc8a
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001223600.L2_SNPP_SST.nc 5fe0bbdd42f9cbbdf7b703554994083876394294
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224200.L2_SNPP_OC.nc ab028c70d33319a686d578698b6dee6d0594c8c6
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224200.L2_SNPP_SST.nc c5b755a607c72ccec0982860bdf44b59ceebfe9d
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224800.L2_SNPP_OC.nc 3eeadccd67e50239dd6761096e1a1298c4c3b10a
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001224800.L2_SNPP_SST.nc 939bfb62f2aad9853e5ed98be6f8554738466744
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001225400.L2_SNPP_OC.nc 2bce3922e625da88320ba77029dfb5aa1b69c1c6
#      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2015001225400.L2_SNPP_SST.nc 4302c478b95fa0c3219f062d2130b4fa2f6fad8c
#
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V20150012*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g hourly
#
# Result in 4 hourly files:
#
#INFO: Created file(s):
#
#      ./viirs_filelist.txt.hourly_2015_001_20_date_2015_01_01 22
#      ./viirs_filelist.txt.hourly_2015_001_21_date_2015_01_01 21
#      ./viirs_filelist.txt.hourly_2015_001_22_date_2015_01_01 22
#      ./viirs_filelist.txt.hourly_2015_001_23_date_2015_01_01 22
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2013-01-01" -e "2013-01-14" -g daily
#
# Result in 14 daily files:
#
#INFO: Created file(s):
#
#      ./viirs_filelist.txt.daily_2013_001_date_2013_01_01 524
#      ./viirs_filelist.txt.daily_2013_002_date_2013_01_02 523
#      ./viirs_filelist.txt.daily_2013_003_date_2013_01_03 524
#      ./viirs_filelist.txt.daily_2013_004_date_2013_01_04 524
#      ./viirs_filelist.txt.daily_2013_005_date_2013_01_05 523
#      ./viirs_filelist.txt.daily_2013_006_date_2013_01_06 525
#      ./viirs_filelist.txt.daily_2013_007_date_2013_01_07 523
#      ./viirs_filelist.txt.daily_2013_008_date_2013_01_08 524
#      ./viirs_filelist.txt.daily_2013_009_date_2013_01_09 524
#      ./viirs_filelist.txt.daily_2013_010_date_2013_01_10 523
#      ./viirs_filelist.txt.daily_2013_011_date_2013_01_11 524
#      ./viirs_filelist.txt.daily_2013_012_date_2013_01_12 524
#      ./viirs_filelist.txt.daily_2013_013_date_2013_01_13 523
#      ./viirs_filelist.txt.daily_2013_014_date_2013_01_14 524
#
#INFO: all_names_found_in_execution 7332 in_files 14

#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-01" -g weekly
#
# Result in 1 weekly file(s):
#
#      ./viirs_filelist.txt.weekly_2015_01_date_2015_01_01 525
#
#INFO: all_names_found_in_execution 525 in_files 1
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-02" -g weekly
#
# Result in 1 weekly file(s):
#
#      ./viirs_filelist.txt.weekly_2015_01_date_2015_01_01 1052
#
#INFO: all_names_found_in_execution 1052 in_files 1
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-01-03" -g weekly
#
#INFO: Created file(s):
#
#      ./viirs_filelist.txt.weekly_2015_01_date_2015_01_01 1577
#
#INFO: all_names_found_in_execution 1577 in_files 1
#
#     % python create_generic_download_list.py -l L2 -t "V*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2015-01-01" -e "2015-03-01" -g weekly
#
# Result in 9 weekly files:
#
#INFO: Created file(s):
#
#
#      ./viirs_filelist.txt.weekly_2015_01_date_2015_01_01 3682
#      ./viirs_filelist.txt.weekly_2015_02_date_2015_01_08 3673
#      ./viirs_filelist.txt.weekly_2015_03_date_2015_01_15 3668
#      ./viirs_filelist.txt.weekly_2015_04_date_2015_01_22 3667
#      ./viirs_filelist.txt.weekly_2015_05_date_2015_01_29 3659
#      ./viirs_filelist.txt.weekly_2015_06_date_2015_02_05 3665
#      ./viirs_filelist.txt.weekly_2015_07_date_2015_02_12 3661
#      ./viirs_filelist.txt.weekly_2015_08_date_2015_02_19 3662
#      ./viirs_filelist.txt.weekly_2015_09_date_2015_02_26 2087
#
#INFO: all_names_found_in_execution 31424 in_files 9
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V2016*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2016-01-01" -e "2016-03-01" -g monthly
#
# Result in 3 month files (Note that the previous command was executed on 3/1/2016 around 4:52 pm, which will have more files added before midnight):
#
#INFO: Created file(s):
#
#      ./viirs_filelist.txt.monthly_2016_01 16677
#      ./viirs_filelist.txt.monthly_2016_02 15753
#      ./viirs_filelist.txt.monthly_2016_03 541
#
# The following execution:
#
#     % python create_generic_download_list.py -l L2 -t "V2016*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc" -n viirs -d 0 -f 1 -a 1 -c 1 -s "2016-01-01" -e "2016-12-31" -g yearly
#
#
#INFO: START_CRAWL: crawl_start_time 1471456394.36 2016-08-17 10:53:14.357717
#create_generic_download_list: Executing query_string http://oceandata.sci.gsfc.nasa.gov/search/file_search.cgi?dtype=L2&search=V2016*[L2_SNPP_OC,L2_SNPP_SST,L2_SNPP_SST3].nc&sensor=viirs&std_only=0&results_as_file=1&addurl=1&cksum=1&sdate=2016-01-01&edate=2016-12-31
#INFO: END_CRAWL: crawl_stop_time 1471456502.09 2016-08-17 10:55:02.090831
#INFO: END_CRAWL: duration_in_seconds 107.73
#INFO: START_SORT: sorting your list of 153899 names into alpha-numeric...
#INFO: END_SORT: duration_in_seconds 0.21
#
#INFO: Created file(s):
#
#      ./viirs_filelist.txt.yearly_2016 121252
#
#INFO: all_names_found_in_execution 121252 in_files 1
#
#
#
# The flag, parameters and their meanings:
#
#      -n search_dtype     = Level 2, only.
#      -t search_filter    = Regular expression of file name.
#      -n search_sensor    = viirs, only
#      -d search_std_only  = Boolean, avoid non standard files
#      -f search_as_file   = Boolean, get the result as a file.
#      -a search_addurl    = Boolean, 1 will prepend "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to the file name.
#      -c checksum_flag    = Boolean, 1 will result in the checksum of the file prepended to filename and a space.  The file name will not have the cgi/getfile prepended
#      -s search_sdate = The start date of the file in yyyy-mm-dd format.
#      -e search_edate = The end   date of the file in yyyy-mm-dd format.
#      -g search_groupby  = ""  # {hourly,daily,weekly,monthly,yearly}
#      -b search_current_only = Only get files that are "current" meaning files from the last 24 hours.
#      -i state_filename = Name of state file to use.
#      -z days_back      = How many days back to search for processing start.  Default is 1.
#
# Debug flag.  Set to 1 if want to see debug statements.
#
# Some notes:
#
#   1.  Because the server may return many names, we have to limit to some regular expression.  The following setting is required for your specific regular expression.
#       Example for MODIS Level 3:
#
#          setenv CRAWLER_SEARCH_FILE_PATTERN "L3m|L3b" 
#
#       Example for VIIRS Level 2:
#
#          setenv CRAWLER_SEARCH_FILE_PATTERN "L2_SNPP_SST|L2_SNPP_SST3|L2_SNPP_OC" 
#
#   2.  Although the -s search_sdate and -e search_edate are optional, it is recommended that they are used since this speed up the search otherwise the search take a long time.
#
#   3.  To turn on the debugger, set CRAWLER_SEARCH_DEBUG_FLAG environment variable to true:
#
#         C-shell method:
#
#           setenv CRAWLER_SEARCH_DEBUG_FLAG true
#
#         Bash method:
#
#           export CRAWLER_SEARCH_DEBUG_FLAG=true
#

def main(argv):
    global g_debug_flag; # Make variable global.
    global g_trace_flag; # Make variable global.
    g_debug_flag = 0     # Change to 1 if want to see debug prints.
    g_trace_flag = 0     # Change to 1 if want to see trace prints.  Typically used by developer to see more of the under the hood.
    g_module_name = 'create_generic_download_list:'

    if (os.getenv("CRAWLER_SEARCH_DEBUG_FLAG") == "true"):
        g_debug_flag = 1
    if (os.getenv("CRAWLER_SEARCH_TRACE_FLAG") == "true"):
        g_trace_flag = 1


    # Define some tokens to search and replace to allow the correct ordering of the SST.nc relatively to SST3.nc granule name.
    # In the natural ordering, SST3.nc comes after SST.nc but we want SST.nc to be the last.  Replace SST.nc with SST9999.nc allows it to be
    # after SST3.nc which is what we want the ordering to be.  The code will replace back SST9999.nc back to SST.nc after the sort routine.

    SST_TOKEN_UNTWEAKED_IN_REPLACE_LOGIC = "L2_SNPP_SST.nc";
    SST_TOKEN_TWEAKED_IN_REPLACE_LOGIC   = "L2_SNPP_SST9999.nc";

    getfile_uri     = "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile"
    search_uri      = "https://oceandata.sci.gsfc.nasa.gov/api/file_search"
    search_dtype    = ""  # L2
    search_filter   = ""  # "V2015001235*SST.nc" (must be inside double quotes)
    search_sensor   = ""  # viirs (This script support viirs sensor only)
    search_std_only = "0" # Boolean, avoid non standard files
    search_as_file  = "1" # Boolean, get the result as a file.
    search_addurl   = "1" # Boolean, 1 will prepend "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to the file name.
    checksum_flag   = "1" # Boolean, 1 will result in the checksum of the file prepended to filename and a space.  This script will do some post processing to produce the correct output.
    search_sdate    = ""  # "2015-01-01" (must be inside double quotes)
    search_edate    = ""  # "2015-01-01" (must be inside double quotes)
    search_groupby  = ""  # {hourly,daily,weekly,monthly,yearly}
    search_current_only_value = "";
    state_filename            = ""; # Name of state file to use.  Default to empty string unless override with -i <state_filename>
    search_psdate   = "" # "2015-01-01" (must be inside double quotes: file processing start date for search)
    search_pedate   = "" # "2015-01-01" (must be inside double quotes: file processing end date for search)
    search_days_back = 1; # How many days back to search for file processing date start.  Default is 1 to get files added the last 24 hours roughly.

    # Define a list of names that this code will only look for and ignore anything else.
    # Add any new patterns you want to add here separate by '|' character.

    # If the user wishes to only look for _SST and _SST3, the following can be set before running this Python script
    #
    #    setenv CRAWLER_SEARCH_FILE_PATTERN "_SST|_SST3"

    default_pattern = "_SST|_SST4|_OC";   # This is the default pattern, look for SST, SST3 and OC files.  _OC files may not be needed.
    if (os.getenv('CRAWLER_SEARCH_FILE_PATTERN','') != ""):
        default_pattern = os.getenv('CRAWLER_SEARCH_FILE_PATTERN','');

    if (g_debug_flag):
        print(g_module_name + "default_pattern[" + default_pattern + "]");

    pattern_to_look_for = re.compile(default_pattern);

    # Get the parameters from command line.

    try:
        opts, args = getopt.getopt(argv,"hl:t:n:d:f:a:c:s:e:o:g:b:i:z:")
    except getopt.GetoptError:
          print('python create_generic_download_list.py -l <dtype> -t <filter> -n <sensor> -d <std_only> -f <as_file> -a <addurl> -c <cksum> -s <sdate> -e <edate> -g <search_groupby> -b <search_current_only_value> -i <state_filename> -z <search_days_back>')
          print('Example')
          print('python create_generic_download_list.py -l l3m -t "A2016241*.nc" -n modis -d 0 -f 1 -a 1 -c 1 -s "2016-08-28" -e "2016-08-28" -g daily')
          sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
             print('test.py -i <inputfile> -o <outputfile>')
             sys.exit()
        elif opt in ("-l"):
             search_dtype = arg
        elif opt in ("-t"):
             search_filter = arg
        elif opt in ("-n"):
             search_sensor = arg
        elif opt in ("-d"):
             search_std_only = arg
        elif opt in ("-f"):
             search_as_file  = arg
        elif opt in ("-a"):
             search_addurl = arg
        elif opt in ("-c"):
             checksum_flag = arg
        elif opt in ("-s"):
             search_sdate = arg
        elif opt in ("-e"):
             search_edate = arg
        elif opt in ("-g"):
             search_groupby = arg # {hourly,daily,weekly,monthly,yearly}
        elif opt in ("-b"):
             search_current_only_value = arg # {crawl_current}
        elif opt in ("-i"):
             state_filename            = arg # Name of state file to use.  This allow the crawling for forward stream to ignore files that it has seen.
        elif opt in ("-z"):
             search_days_back = int(arg) # How many days back to search for file processing date start.  Default is 1 

    encountered_error_flag = False;
    # For yearly search, we can provided the ability to split up the query by month so as not to time out if that particular dataset is too big..
    if (search_groupby == "yearly"):
        # Split the search start date and search end date parameters into individual months.
        (o_search_sdates,o_search_edates) = generic_split_search_dates_into_months(search_sdate,search_edate);
        print("search_sdate",search_sdate);
        print("search_edate",search_edate);
        print("o_search_sdates",o_search_sdates);
        print("o_search_edates",o_search_edates);
        print("");

    
        processing_loop = 0;
        max_loop = len(o_search_sdates); 
        while ((not encountered_error_flag) and (processing_loop < max_loop)):
            search_sdate = o_search_sdates[processing_loop];
            search_edate = o_search_edates[processing_loop];

            if (os.getenv("CRAWLER_SEARCH_SKIP_ACTUAL_DOWNLOAD","") == "true"):
                print("CRAWLER_SEARCH_SKIP_ACTUAL_DOWNLOAD is true.  No downloading.")
                print("    processing_loop",processing_loop,"max_loop",processing_loop,"search_sdate",search_sdate,"search_edate",search_edate);
            else:
                encountered_error_flag = create_generic_download_list(search_dtype,       # L2
                                                                      search_filter,      # "V2015001235*SST.nc" (must be inside double quotes)
                                                                      search_sensor,      # viirs (This script support viirs sensor only)
                                                                      search_std_only,    # Boolean, avoid non standard files
                                                                      search_as_file,     # Boolean, get the result as a file.
                                                                      search_addurl,      # Boolean, 1 will prepend "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to the file name.
                                                                      checksum_flag,      # Boolean, 1 will result in the checksum of the file prepended to filename and a space.  This script will do some post processing to produce the correct output.
                                                                      search_sdate,       # "2015-01-01" (must be inside double quotes)
                                                                      search_edate,       # "2015-01-01" (must be inside double quotes)
                                                                      search_groupby,     # {hourly,daily,weekly,monthly,yearly}
                                                                      search_current_only_value,    
                                                                      state_filename,     # Name of state file to use.  Default to empty string unless override with -i <state_filename>
                                                                      search_psdate,      # "2015-01-01" (must be inside double quotes: file processing start date for search)
                                                                      search_pedate,      # "2015-01-01" (must be inside double quotes: file processing end date for search)
                                                                      search_days_back,   # How many days back to search for file processing date start.  Default is 1 to get files added the last 24 hours roughly.
                                                                      pattern_to_look_for);
            # end else portion of if (os.getenv("CRAWLER_SEARCH_SKIP_ACTUAL_DOWNLOAD","") == "true")
            processing_loop += 1;
        # end while ((not encountered_error_flag) and (processing_loop < max_loop))

#        # For now, exit after 2 iterations.
#        if (processing_loop >= 1):
#            encountered_error_flag = True;

    # end while not encountered_error_flag
    else:
        encountered_error_flag = create_generic_download_list(search_dtype,       # L2
                                                              search_filter,      # "V2015001235*SST.nc" (must be inside double quotes)
                                                              search_sensor,      # viirs (This script support viirs sensor only)
                                                              search_std_only,    # Boolean, avoid non standard files
                                                              search_as_file,     # Boolean, get the result as a file.
                                                              search_addurl,      # Boolean, 1 will prepend "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to the file name.
                                                              checksum_flag,      # Boolean, 1 will result in the checksum of the file prepended to filename and a space.  This script will do some post processing to produce the correct output.
                                                              search_sdate,       # "2015-01-01" (must be inside double quotes)
                                                              search_edate,       # "2015-01-01" (must be inside double quotes)
                                                              search_groupby,     # {hourly,daily,weekly,monthly,yearly}
                                                              search_current_only_value,    
                                                              state_filename,     # Name of state file to use.  Default to empty string unless override with -i <state_filename>
                                                              search_psdate,      # "2015-01-01" (must be inside double quotes: file processing start date for search)
                                                              search_pedate,      # "2015-01-01" (must be inside double quotes: file processing end date for search)
                                                              search_days_back,   # How many days back to search for file processing date start.  Default is 1 to get files added the last 24 hours roughly.
                                                              pattern_to_look_for);

    # Depend on if we had encountered an error or not, we exit with the appropriate code so an external program can decide what to do.
    if (encountered_error_flag):
        sys.exit(0);
    else:
        sys.exit(1);


def create_generic_download_list(search_dtype,       # L2
                                 search_filter,      # "V2015001235*SST.nc" (must be inside double quotes)
                                 search_sensor,      # viirs (This script support viirs sensor only)
                                 search_std_only,    # Boolean, avoid non standard files
                                 search_as_file,     # Boolean, get the result as a file.
                                 search_addurl,      # Boolean, 1 will prepend "http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/" to the file name.
                                 checksum_flag,      # Boolean, 1 will result in the checksum of the file prepended to filename and a space.  This script will do some post processing to produce the correct output.
                                 search_sdate,       # "2015-01-01" (must be inside double quotes)
                                 search_edate,       # "2015-01-01" (must be inside double quotes)
                                 search_groupby,     # {hourly,daily,weekly,monthly,yearly}
                                 search_current_only_value,
                                 state_filename,     # Name of state file to use.  Default to empty string unless override with -i <state_filename>
                                 search_psdate,      # "2015-01-01" (must be inside double quotes: file processing start date for search)
                                 search_pedate,      # "2015-01-01" (must be inside double quotes: file processing end date for search)
                                 search_days_back,   # How many days back to search for file processing date start.  Default is 1 to get files added the last 24 hours roughly.
                                 pattern_to_look_for):



    global g_debug_flag; # Make variable global.
    global g_trace_flag; # Make variable global.
    g_debug_flag = 0     # Change to 1 if want to see debug prints.
    g_trace_flag = 0     # Change to 1 if want to see trace prints.  Typically used by developer to see more of the under the hood. 
    g_module_name = 'create_generic_download_list:'

    if (os.getenv("CRAWLER_SEARCH_DEBUG_FLAG") == "true"):
        g_debug_flag = 1
    if (os.getenv("CRAWLER_SEARCH_TRACE_FLAG") == "true"):
        g_trace_flag = 1

    o_encountered_error_flag = False;

    # Define some tokens to search and replace to allow the correct ordering of the SST.nc relatively to SST3.nc granule name.
    # In the natural ordering, SST3.nc comes after SST.nc but we want SST.nc to be the last.  Replace SST.nc with SST9999.nc allows it to be
    # after SST3.nc which is what we want the ordering to be.  The code will replace back SST9999.nc back to SST.nc after the sort routine.
 
    SST_TOKEN_UNTWEAKED_IN_REPLACE_LOGIC = "L2_SNPP_SST.nc";
    SST_TOKEN_TWEAKED_IN_REPLACE_LOGIC   = "L2_SNPP_SST9999.nc";

    getfile_uri     = "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile"
    search_uri      = "https://oceandata.sci.gsfc.nasa.gov/api/file_search"


    # Special processing if the value of search_current_only_value is "get_current_files_only"
    g_state_dictionary = {};  # Define a dictionary so we can save all files' state to this dictionary.  A state of a file is a name plus checksum -> http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/V2016001000000.L2_SNPP_OC.nc 466cbc5de5c8286a9723eb3c935c20aa98eabbc0
    g_state_for_saving_dictionary = {}; # Define a dictionary so we can save the current state.  This allows us to have a state of only recent files.  It doesn't carry over.
    g_use_state_flag = False;
    g_num_names_added_to_state = 0;
    g_num_names_replaced_in_state = 0;
    g_num_existing_names_same_checksum_in_state      = 0;
    g_num_existing_names_different_checksum_in_state = 0;
    g_num_names_found_from_crawling = 0;# A raw number of names found from crawling.
    g_num_names_matching_pattern    = 0;   # Number of file names matching pattern_to_look_for object.
    g_num_names_dissimilar_pattern  = 0; # Number of file names different than pattern_to_look_for object.
    g_num_file_states_loaded        = 0;
    default_state_filename = "";

    # The crawl_current has special processing:
    #     1.  This crawling allows the crawler to pick up files that has been processed within a window of time.
    #     2.  If a new files has been added since last crawl, only the new file names will be returned.
    #     3.  It uses a state file to keep track of file names it has seen before.

    if (search_current_only_value == "crawl_current"):
        g_use_state_flag = True;
        if (g_debug_flag):
            print(g_module_name + "search_days_back",search_days_back)
        window_start_date = datetime.datetime.now() - datetime.timedelta(days=search_days_back);   # The window start is that many days back.

        time_search_option = "use_date_when_file_processed";  # TIME_SEARCH_OPTION_1  Use the processing date of the file to search.
#        time_search_option = "use_date_in_filename";    # TIME_SEARCH_OPTION_2  Use the date in the file name to search.

        # TIME_SEARCH_OPTION_1: To make sure we get files that has been processed within a window, we only search for files that have been processed since window start to now.
        #                       By default, the window start is one day ago, unless the -z option is specified then we get files that has been processed that many days back.
        if (time_search_option == "use_date_when_file_processed"):
            from datetime import date;
            search_psdate   = window_start_date.strftime("%Y-%m-%d"); 
            #search_pedate   = time.strftime("%Y-%m-%d"); 
            # For some strange reason, using today's date does not return any files.
            # Try adding 1 day to today's date so it will tomorrow.
            weird_end_date = date.today() + datetime.timedelta(days=2); 
            search_edate = weird_end_date.strftime("%Y-%m-%d");
            print(g_module_name + "search_psdate["+search_psdate+"]")
            print(g_module_name + "search_edate["+search_edate+"]")
            print(g_module_name + "time_search_option["+time_search_option+"]")
            #exit(0);

        # TIME_SEARCH_OPTION_2: To make sure we get files that has time from the specified start date, we only search for files from the specified start date to midnight today.
        if (time_search_option == "use_date_in_filename"):
            # If the user has not specify the -s <sdate> we get it from the window start.
            if (search_sdate == ""):
                search_sdate = window_start_date.strftime("%Y-%m-%d");

            # If the user has not specify the -e <edate> we get it from the today's date.
            if (search_edate == ""):
                search_edate = time.strftime("%Y-%m-%d");

        default_state_filename = "viirs_daily_current_state.txt";

        if (state_filename != ""):
            default_state_filename = state_filename;

        if (g_debug_flag):
            print(g_module_name + "search_current_only_value",search_current_only_value);
            print(g_module_name + "search_sdate",search_sdate);
            print(g_module_name + "search_edate",search_edate);
            print(g_module_name + "search_psdate",search_psdate);
            print(g_module_name + "search_pedate",search_pedate);
            print(g_module_name + "time_search_option",time_search_option);
            print(g_module_name + "state_filename",state_filename);
            print(g_module_name + "default_state_filename",default_state_filename);
#        sys.exit(0);

        # Load the content of the default state file into a dictionary.
        if os.path.isfile(default_state_filename):
            if (g_debug_flag):
                print(g_module_name + "loading default_state_filename",default_state_filename)

#            g_debug_flag = 1;
            with open(default_state_filename) as input_file_fp:
                 for line in input_file_fp:
                    g_num_file_states_loaded += 1;
                    (key, val) = line.rstrip().split();
                    g_state_dictionary[key] = val;  # Read from state file into g_state_dictionary so we can have something to check for a file when we see it.

                    if (g_num_file_states_loaded != len(g_state_dictionary)):
                        print("ERROR:default_state_filename",default_state_filename,"g_num_file_states_loaded",g_num_file_states_loaded,"len(g_state_dictionary)",len(g_state_dictionary),"two_values_differ");
                        input_file_fp.close();
                        sys.exit(0);
                     
                    if (g_trace_flag):
                        print("line",line.rstrip());
                        print("key",key,"val",val)
            input_file_fp.close();
            if (g_debug_flag):
                print(g_module_name + "loaded default_state_filename",default_state_filename,"g_num_file_states_loaded",g_num_file_states_loaded,"len(g_state_dictionary)",len(g_state_dictionary));
        else:
            print(g_module_name + "WARN:FILE_NOT_EXIST:default_state_filename",default_state_filename,"does not exist yet.  Starting with zero state.");
#        sys.exit(0);

    if (g_debug_flag):
        print(g_module_name + "search_sdate",search_sdate);
        print(g_module_name + "search_edate",search_edate);

#    exit(0);

    # Do a sanity check on all parameters.

    if (search_dtype == ""):
          print('ERROR: Must specify option: -l <dtype>')
          print('python create_generic_download_list.py -l <dtype> -t <filter> -n <sensor> -d <std_only> -f <as_file> -a <addurl> -c <cksum> -s <sdate> -e <edate> -g <search_groupby>')
          print('Example: -l L2')
          sys.exit(2)
    if (search_filter == ""):
          print('ERROR: Must specify option: -t <filter>')
          print('python create_generic_download_list.py -l <dtype> -t <filter> -n <sensor> -d <std_only> -f <as_file> -a <addurl> -c <cksum> -s <sdate> -e <edate> -g <search_groupby>')
          print('Example: -t "T2015001235*SST.nc"')
          sys.exit(2)
    if (search_sensor == ""):
          print('ERROR: Must specify option: -n <sensor>')
          print('python create_generic_download_list.py -l <dtype> -t <filter> -n <sensor> -d <std_only> -f <as_file> -a <addurl> -c <cksum> -s <sdate> -e <edate> -g <search_groupby>')
          print('Example: -n viirs')
          sys.exit(2)

    valid_groupby_list = ["hourly","daily","weekly","monthly","yearly"];

    if (search_groupby not in valid_groupby_list):
          print('ERROR: Must specify a valid value for -g option:')
          print('Valid options are',valid_groupby_list);
          print('You specified',search_groupby);
          sys.exit(2)

    # Do a sanity check on the search_filter to see that it at least contain the year
    # For some strange reason, the search query behaves badly when you don't give it a year, i.e.
    # it will not give you any result and you will be frustrated wondering why this thing won't work.

#    validate_search_filter(search_filter);

    # Check to see if search_sdate and search_edate are provided and add them to the query_string.
    query_string = ""

    # Because we added the ability to search for files that has been processed within a window, the logic gets a little tricky
    # as you have to keep track of these four parameters while building the query_string variable:
    #
    #   the start window of file granule start time
    #   the end   window of file granule start time
    #   the start window of a file processing time
    #   the end   window of a file processing time
    if (search_sdate != ""): 
        if (g_debug_flag):
            print(g_module_name + "SEARCH_SDATE_NOT_EMPTY");
        if (search_edate != ""): 
            if (g_debug_flag):
                print(g_module_name + "SEARCH_EDATE_NOT_EMPTY");
            if (search_psdate != ""): 
                if (g_debug_flag):
                    print(g_module_name + "SEARCH_PSDATE_NOT_EMPTY");
                if (search_pedate != ""): 
                    if (g_debug_flag):
                        print(g_module_name + "SEARCH_PSDATE_NOT_EMPTY:SEARCH_PEDATE_NOT_EMPTY");
                    query_string = search_uri + "?" + "dtype=" + search_dtype + "&search=" + search_filter + "&sensor=" + search_sensor + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag + "&sdate=" + search_sdate + "&edate=" + search_edate + "&psdate=" +  search_psdate + "&pedate" + search_pedate
                else: # search_pedate is empty string.
                    if (g_debug_flag):
                         print(g_module_name + "SEARCH_PSDATE_NOT_EMPTY:SEARCH_PEDATE NOT EMPTY");
                    query_string = search_uri + "?" + "dtype=" + search_dtype + "&search=" + search_filter + "&sensor=" + search_sensor + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag + "&sdate=" + search_sdate + "&edate=" + search_edate + "&psdate=" +  search_psdate;
            else: # search_psdate is empty string.
                if (g_debug_flag):
                     print(g_module_name + "SEARCH_EDATE_NOT_EMPTY:SEARCH_PSDATE_EMPTY");
                query_string = search_uri + "?" + "dtype=" + search_dtype + "&search=" + search_filter + "&sensor=" + search_sensor + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag + "&sdate=" + search_sdate + "&edate=" + search_edate;
        else: # search_edate is empty string
            if (g_debug_flag):
                print(g_module_name + "SEARCH_SDATE_NOT_EMPTY:SEARCH_EDATE_EMPTY");
            query_string = search_uri + "?" + "dtype=" + search_dtype + "&search=" + search_filter + "&sensor=" + search_sensor + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag + "&sdate=" + search_sdate;
    else: # search_sdate is empty string.
        # If it acceptable to have the search_sdate (meaning that the user does not care about when the granule started), but they should specified the search_filter to something.
        if (g_debug_flag):
            print(g_module_name + "SEARCH_SDATE_EMPTY");
        if (search_psdate != ""):
            if (g_debug_flag):
                print(g_module_name + "SEARCH_PSDATE_NOT_EMPTY");
            if (search_pedate != ""):
                if (g_debug_flag):
                    print(g_module_name + "SEARCH_SDATE_EMPTY:SEARCH_PSDATE_NOT_EMPTY:SEARCH_PEDATE NOT EMPTY");
                query_string = search_uri + "?" + "dtype=" + search_dtype + "&search=" + search_filter + "&sensor=" + search_sensor + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag + "&psdate=" +  search_psdate + "&pedate=" +  search_pedate;
            else:
                if (g_debug_flag):
                    print(g_module_name + "SEARCH_SDATE_EMPTY:SEARCH_PSDATE_NOT_EMPTY:SEARCH_PEDATE_EMPTY");
                #print(g_module_name + "search_uri",search_uri,type(search_uri))
                #print(g_module_name + "search_dtype",search_dtype,type(search_uri))
                #print(g_module_name + "search_filter",search_filter,type(search_filter))
                #print(g_module_name + "search_sensor",search_sensor,type(search_sensor))
                #print(g_module_name + "search_std_only",search_std_only,type(search_std_only))
                #print(g_module_name + "search_as_file",search_as_file,type(search_as_file))
                #print(g_module_name + "search_addurl",search_addurl,type(search_addurl))
                #print(g_module_name + "checksum_flag",checksum_flag,type(checksum_flag))
                #print(g_module_name + "search_psdate",search_psdate,type(search_psdate))

                #print("search_uri='"+str(search_uri)+"'")
                #print("search_dtype='"+str(search_dtype)+"'")
                #print("search_filter='"+str(search_filter)+"'")
                #print("search_sensor='"+str(search_sensor)+"'")
                #print("search_std_only='"+str(search_std_only)+"'")
                #print("search_as_file='"+str(search_as_file)+"'")
                #print("search_addurl='"+str(search_addurl)+"'")
                #print("checksum_flag='"+str(checksum_flag)+"'")
                #print("search_psdate='"+str(search_psdate)+"'")

                query_string = search_uri + "?" + "dtype=" + search_dtype + "&search=" + search_filter + "&sensor=" + search_sensor + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag + "&psdate=" +  search_psdate;

        else: # if (search_psdate == ""):
            if (g_debug_flag):
                print(g_module_name + "SEARCH_SDATE_EMPTY:SEARCH_PSDATE_EMPTY");
            query_string = search_uri + "?" + "dtype=" + search_dtype + "&search=" + search_filter + "&sensor=" + search_sensor + "&std_only=" + search_std_only + "&results_as_file=" + search_as_file + "&addurl=" + search_addurl + "&cksum=" + checksum_flag;
    
    if (g_debug_flag):
        print(g_module_name + "query_string [" + query_string + "]")

    # Get the default output directory if provided, otherwise, use the current directory.

    CRAWLER_DEFAULT_OUTPUT_DIRECTORY = "./";

    if (os.getenv("CRAWLER_SEARCH_DEFAULT_OUTPUT_DIRECTORY","") != ""):
        if (os.getenv("CRAWLER_SEARCH_DEFAULT_OUTPUT_DIRECTORY","").endswith("/")):
            CRAWLER_DEFAULT_OUTPUT_DIRECTORY = os.getenv("CRAWLER_SEARCH_DEFAULT_OUTPUT_DIRECTORY");
        else:
            CRAWLER_DEFAULT_OUTPUT_DIRECTORY = os.getenv("CRAWLER_SEARCH_DEFAULT_OUTPUT_DIRECTORY") + "/";
        # Check if directory exists.  If not, create it.
        if (not os.path.isdir(CRAWLER_DEFAULT_OUTPUT_DIRECTORY)):
            print(g_module_name, 'DIR_CREATE',CRAWLER_DEFAULT_OUTPUT_DIRECTORY);
            os.mkdir(CRAWLER_DEFAULT_OUTPUT_DIRECTORY);

    # Fetch the content from the query.

    if (g_debug_flag):
        print('os.getenv("CRAWLER_SEARCH_TEST_MODE")[',os.getenv("CRAWLER_SEARCH_TEST_MODE",""),']');

    crawl_start_time = time.time();
    print(g_module_name + 'INFO:START_CRAWL:crawl_start_time',crawl_start_time, str(datetime.datetime.now()));

#    g_trace_flag = 1;
    if (os.getenv("CRAWLER_SEARCH_TEST_MODE") == "true"):
        # For developer only:  Since we are running a test, we want to set these values as hard-coded.
        # The content is a large string with carriage returns.

        print('os.getenv("CRAWLER_SEARCH_TEST_MODE")[',os.getenv("CRAWLER_SEARCH_TEST_MODE",""),']');
        content_raw = \
                   '6aadb70909b95a0ba139b2ece0240bee1e9531a1 V2015001200000.L2_SNPP_SST.nc\n'  + \
                   'd91ab88270cf8dbff745ad0f96da05427fed56fe V2015001200500.L2_SNPP_SST.nc\n'  + \
                   '953ea181edcd8bfdbcf9402b68be46b37054fd69 V2015001201000.L2_SNPP_SST.nc\n'  + \
                   '8064d036c26a6a0cfc67a7eb694148c7fe248d52 V2015001201500.L2_SNPP_SST.nc\n'  + \
                   '1a699df687f625bc6ab0a1e0f990898626581cf0 V2015001202000.L2_SNPP_SST.nc\n'  + \
                   'f5bcbb45e6a25749969d2ba985027f83e34490f1 V2015001202500.L2_SNPP_SST.nc\n'  + \
                   '25e37084e51bb1bac49920eee39550b0b3bb262f V2015001203000.L2_SNPP_SST.nc\n'  + \
                   'a77eb9e2274dc43a4a7d06d9ac7114b74ad40c91 V2015001203500.L2_SNPP_SST.nc\n'  + \
                   '94bcd3d5ee88fdc9f27ec8646bc04201351a08b4 V2015001204000.L2_SNPP_SST.nc\n'  + \
                   '2c8c94f7d37eb36d70e2ada526460e88604c5657 V2015001204000.L2_SNPP_SST3.nc\n' + \
                   'ffd75043967a6dd9f9400ed84c6d859a8cc04660 V2015001204500.L2_SNPP_SST.nc\n'  + \
                   '8a61a0df1c9af563603b364020731a562ce7a689 V2015001204500.L2_SNPP_SST3.nc\n' + \
                   'd69d4d93e474ae6d2421998b78116fa305fe2679 V2015001205000.L2_SNPP_SST.nc\n'  + \
                   '988068a80e3dfc8e29772937529414f5c72229ad V2015001205000.L2_SNPP_SST3.nc\n' + \
                   'ce807d2aa9fffd11f0a6a4feb6301c045564aee9 V2015001205500.L2_SNPP_SST.nc\n'  + \
                   '0674ea50c697834d4dc8a857b334728fa28267b4 V2015001205500.L2_SNPP_SST3.nc\n' + \
                   'zzz4ea50c697834d4dc8a857b334728fa2826zzz V2015001210000.L2_SNPP_SST.nc\n'  + \
                   'zzz4ea50c697834d4dc8a857b334728fa2826zzz V2016001210000.L2_SNPP_SST.nc\n';

    else:
        # The content returned from the read() function is a large string with carriage return.

        print(g_module_name + 'INFO:Executing query_string', query_string);
        #exit(0);

        content_raw = requests.get(query_string)
    crawl_stop_time= time.time();
    print(g_module_name + 'INFO:END_CRAWL:crawl_stop_time',crawl_stop_time,str(datetime.datetime.now()));
    print(g_module_name + 'INFO:END_CRAWL:duration_in_seconds %.2f' % (crawl_stop_time - crawl_start_time));
    time.sleep(3) # Sleep for 3 seconds so the user can see how long the query took.

    if (g_trace_flag):
        print(g_module_name + "TRACE:content_raw [" + content_raw + "]")
        print(g_module_name + "query_string [" + query_string + "]")
#    sys.exit(0);


    content_as_list_unsorted = content_raw.text.split('\n')

    # Because OBPG does not have these names in any order we can depend on, we have to sort them.
    # Also, the list starts with the checksum, then the name:
    #
    # 02cd518f8f2e9cfbd2ec4a0fc1cadcda6ae0c318  V2016016120000.L2_SNPP_SST3.nc
    # 011d8da0d15f080098d75efc9a9680056ae989b4  V2016010043500.L2_SNPP_SST3.nc
    # f282221294648c47049e2478001c582388d83e87  V2016013101000.L2_SNPP_SST.nc
    # 9e9064a8c1566319cf6609f008b73a20a2c2d5f5  V2016018022000.L2_SNPP_SST.nc
    # e5e4238636d033fefcc09ecc79cb7110015e9e8a  V2016018205500.L2_SNPP_SST.nc
    #
    # We have to swap the columns, sort them using the file name, then write 

    # Split each line, into checkum and file name components, then save them into a dictionary. 
    new_content_with_names_and_checksum_switched = [];

    # Only process if the line contains the regular expression.
    regular_expression_to_check = os.getenv("CRAWLER_SEARCH_FILE_PATTERN","");
    for one_line in content_as_list_unsorted:
        if (g_debug_flag):
            print(g_module_name + "regular_expression_to_check",regular_expression_to_check);
            print(g_module_name + "one_line[",one_line,"]");
            print(g_module_name + "bool(re.search(regular_expression_to_check, one_line))",bool(re.search(regular_expression_to_check, one_line)));
        if (regular_expression_to_check == ""):
            print(g_module_name + "Nothing to check for from CRAWLER_SEARCH_FILE_PATTERN");
            sys.exit(0);

        if bool(re.search(regular_expression_to_check, one_line)):
            # Parse the line into 2 tokens.
            tokens = re.findall(r'[^"\s]\S*|".+?"', one_line)
            if (len(tokens) >= 2):
                checksum_part = tokens[0];
                filename_part = tokens[1];

                # Because in the natural order of the list of files below, the SST comes before SST3:
                #
                #     A2013001232000.L2_SNPP_OC.nc
                #     A2013001232000.L2_SNPP_SST.nc
                #     A2013001232000.L2_SNPP_SST3.nc
                #
                # But we wish the SST to be the last name, so we tweak the L2_SNPP_SST.nc to L2_SNPP_SST9999 so that it is last.

                tweaked_filename = filename_part;
                if (filename_part.find(SST_TOKEN_UNTWEAKED_IN_REPLACE_LOGIC) >= 0):
                    tweaked_filename = filename_part.replace(SST_TOKEN_UNTWEAKED_IN_REPLACE_LOGIC,SST_TOKEN_TWEAKED_IN_REPLACE_LOGIC);

                new_line = tweaked_filename + " " + checksum_part
                if (g_trace_flag):
                    print(g_module_name + 'Adding:new_content_with_names_and_checksum_switched:',new_line);
                new_content_with_names_and_checksum_switched.append(new_line);
            # end if (len(tokens) >= 2):
        # end bool(re.search(regular_expression_to_check, one_line)) 
    # end for one_line in content_as_list_unsorted:

    if (g_trace_flag):
        print(g_module_name + 'new_content_with_names_and_checksum_switched',new_content_with_names_and_checksum_switched);



    print(g_module_name + 'INFO:START_SORT:sorting your list of',len(new_content_with_names_and_checksum_switched),'names into alpha-numeric...');
    sort_start_time = time.time();
    content_as_list_sorted_but_with_tweaked_names = sorted(new_content_with_names_and_checksum_switched);
    sort_stop_time= time.time();
    print(g_module_name + 'INFO:END_SORT:duration_in_seconds %.2f' % (sort_stop_time - sort_start_time));
    print(g_module_name + 'content_as_list_sorted_but_with_tweaked_names ');
#    sys.exit(0);

    # Now that the list has been sorted, we have to replace the tweaked names back to the untweak name, i.e. replace A2013001231500.L2_SNPP_SST9999.nc with A2013001231500.L2_SNPP_SST.nc

    content_as_list_sorted = [];

    for one_line in content_as_list_sorted_but_with_tweaked_names:
       # But we wish the SST to be the last name, we tweak the L2_SNPP_SST.nc to L2_SNPP_SST9999 so that it is last.
       original_line = one_line;
       if (one_line.find(SST_TOKEN_TWEAKED_IN_REPLACE_LOGIC) >= 0):
           original_line = one_line.replace(SST_TOKEN_TWEAKED_IN_REPLACE_LOGIC,SST_TOKEN_UNTWEAKED_IN_REPLACE_LOGIC);
           if (g_trace_flag):
               print(g_module_name + 'Adding:content_as_list_sorted:',original_line);
       content_as_list_sorted.append(original_line);
#    sys.exit(0);

    # For each line in the list, parse the tokens, swap the first and second column and write the tokens back out to the download list.

    found_names = 0;
    all_names_found_in_execution = 0;
    first_name_found_flag = False;

    # We start with a default file name and then it will be appended with specific verbase as to which type of file it is: yearly, monthly, weekly, daily, hourly, etc...
    output_file_pointer = 'DUMMY_OUTPUT_FILE_POINTER';
    previous_output_file_name = '';
    # The aqua or terra sensor has "modis_" preceed in the file name to keep consistency with the modis-rdac handler.
    if (search_sensor == "aqua" or search_sensor == "terra"):
        output_file_name = CRAWLER_DEFAULT_OUTPUT_DIRECTORY + "modis_" + search_sensor + '_filelist.txt';
    else:
        output_file_name = CRAWLER_DEFAULT_OUTPUT_DIRECTORY + search_sensor + '_filelist.txt';
    BASE_OUTPUT_FILE_NAME = output_file_name;

#    if (search_sensor == 'modis'):
#        output_file_name = CRAWLER_DEFAULT_OUTPUT_DIRECTORY + 'viirs_filelist.txt';
#        BASE_OUTPUT_FILE_NAME = output_file_name;
#    else:
#        print "This script only support sensor modis";
#        sys.exit(0);

#    print "BASE_OUTPUT_FILE_NAME",BASE_OUTPUT_FILE_NAME;
#    sys.exit(0);

    # Use this list to save the list of output file names so we can write them out to user.
    list_of_output_file_names = [];

    current_year   = 'DUMMY_CURRENT_YEAR';
    previous_year  = 'DUMMY_PREVIOUS_YEAR';
    current_month  = 'DUMMY_CURRENT_MONTH';
    previous_month = 'DUMMY_PREVIOUS_MONTH';
    current_week   = 'DUMMY_CURRENT_WEEK';
    previous_week  = 'DUMMY_PREVIOUS_WEEK';
    current_day_of_year  = 'DUMMY_CURRENT_DAY_OF_YEAR';
    previous_day_of_year = 'DUMMY_PREVIOUS_DAY_OF_YEAR';
    current_hour  = 'DUMMY_CURRENT_HOUR';
    previous_hour = 'DUMMY_PREVIOUS_HOUR';

    # As each file is inspected against the state file, we determine if we have seen this file before.  
    # If the file has not been seen before, we set the state to "ready_for_saving"
    # If the file has been seen before:
    #      but the checksum has been modified, we set the state to "ready_for_saving".
    #      If the checksum is the same, we set the ste to          "seen_before_with_same_checksum"
    # Only the file with the state "ready_for_saving" will be written to file.

    file_state_status = "";

    g_num_added_to_saving_dictionary = 0;
    # Only process if the line contains the regular expression.
    regular_expression_to_check = os.getenv("CRAWLER_SEARCH_FILE_PATTERN","");

    for one_line in content_as_list_sorted:
        file_state_status = "undefined_state";   # Start out with an undefined_state and will be either set to "ready_for_saving" or "seen_before_with_same_checksum";
        # Each line look like this and we will need to swap the columns and prepend 'http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/' to the file name.
        #
        # 63a5273e0220da71cdc91a9753907eb47d03ac83  T2015001235000.L2_SNPP_SST.nc
        # b99acbb3b5f3996d860c93f07cde3632df7b9617  T2015001235500.L2_SNPP_SST.nc

        if (g_trace_flag):
            print(g_module_name + "TRACE:one_line [ " +  one_line + "]")

        if (g_debug_flag):
            print(g_module_name + "regular_expression_to_check",regular_expression_to_check);
            print(g_module_name + "one_line",one_line);
            print(g_module_name + "bool(re.search(regular_expression_to_check, one_line))",bool(re.search(regular_expression_to_check, one_line)));
        if (regular_expression_to_check == ""):
            print(g_module_name + "Nothing to check for from CRAWLER_SEARCH_FILE_PATTERN");
            sys.exit(0);

        if bool(re.search(regular_expression_to_check, one_line)):
            g_num_names_found_from_crawling += 1;

            # Parse the line into 2 tokens.

            tokens = re.findall(r'[^"\s]\S*|".+?"', one_line)

            if (len(tokens) >= 2):
                # The order of the tokens are already in the right order: filename_part checksum_part
                filename_part = tokens[0]
                checksum_part = tokens[1]
                if (g_trace_flag):
                    print(g_module_name + "TRACE:checksum_part [" + checksum_part + "]")
                    print(g_module_name + "TRACE:filename_part [" + filename_part + "]")
    
                # Write the tokens out in reverse order with one space in between.
                #
                # Prepend the getfile_uri since the query does not return the full url eventhough we requested it with addurl=1 
                #
                #      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/T2015001235000.L2_SNPP_SST.nc 63a5273e0220da71cdc91a9753907eb47d03ac83
                #      http://oceandata.sci.gsfc.nasa.gov/cgi/getfile/T2015001235500.L2_SNPP_SST.nc b99acbb3b5f3996d860c93f07cde3632df7b9617
    
                # Only look for file that contains "SST", "SST3" or "OC" and exclude anything else.

                if (re.search(pattern_to_look_for,filename_part)):
                    g_num_names_matching_pattern += 1; # Number of file names matching pattern_to_look_for object.
                    new_line = getfile_uri + "/" + filename_part + " " + checksum_part
                    name_with_uri = getfile_uri + "/" + filename_part;
                    # If we are not using the state, the default state is "ready_for_saving"
                    if (g_use_state_flag == 0):
                        file_state_status = "ready_for_saving";   # If not using state file, we set file_state_status to "ready_for_saving" because we want to save the file name to file. 
                    # If we are to use the state file, we check to see if the name has been seen before.
                    if (g_use_state_flag):
                       if name_with_uri in g_state_dictionary:    # Check the file name in g_state_dictionary to see if we have seen it before.
                           if (g_trace_flag):
                                print(g_module_name + "TRACE:NAME_INDEED_IN_STATE",name_with_uri,"len(g_state_dictionary)",len(g_state_dictionary));

                           if (g_state_dictionary[name_with_uri] == checksum_part): # Check the checksum in g_state_dictionary to see if it is the same or had the file been modified since we last saw it.
                               if (g_trace_flag):
                                   print(g_module_name + "TRACE:FOUND_NAME_IN_STATE",name_with_uri,'WITH_SAME_CHECKSUM',g_state_dictionary[name_with_uri],'SKIPPING');
                               g_num_existing_names_same_checksum_in_state = g_num_existing_names_same_checksum_in_state + 1;
                               file_state_status = "seen_before_with_same_checksum";
                               if (g_trace_flag):
                                   print(g_module_name + "TRACE:SKIPPING",g_num_existing_names_same_checksum_in_state,name_with_uri);
                               # Regardless if we have seen this file before or not, we save its state to g_state_for_saving_dictionary for keep the state currrent.
                               g_state_for_saving_dictionary[name_with_uri] = checksum_part; # Save to g_state_for_saving_dictionary so we can keep the state current, i.e the state doesn't grow beyond today's files.
                               g_num_added_to_saving_dictionary += 1;
                               if (g_trace_flag):
                                   print(g_module_name + "TRACE:ADDING_TO_SAVING_DICTIONARY",g_num_added_to_saving_dictionary,name_with_uri,checksum_part);
                               continue;
                           else:
                               if (g_trace_flag):
                                   print(g_module_name + "TRACE:WARN:FOUND_NAME_IN_STATE",name_with_uri,'WITH_DIFFERENT_CHECKSUM',checksum_part);
                               g_state_dictionary[name_with_uri] = checksum_part;  # Save to g_state_dictionary eventhough we have seen it before but the checksum is different.
                               g_num_names_replaced_in_state += 1;
                               g_num_existing_names_different_checksum_in_state = g_num_existing_names_different_checksum_in_state + 1;
                               file_state_status = "ready_for_saving";  # Seen before with different checksum.
                               if (g_trace_flag):
                                   print(g_module_name + "TRACE:WARN:ADDING_TO_STATE",g_num_existing_names_different_checksum_in_state,name_with_uri,checksum_part);
#                           sys.exit(0);
                       else:
                           if (g_trace_flag):
                               print(g_module_name + "TRACE:NAME_NOT_IN_STATE",name_with_uri,"len(g_state_dictionary)",len(g_state_dictionary));
                           g_state_dictionary[name_with_uri] = checksum_part;      # Save to g_state_dictionary because we have not seen this file before.
                           g_num_names_added_to_state = g_num_names_added_to_state + 1;
                           file_state_status = "ready_for_saving";  # Seen before with different checksum.
                           if (g_trace_flag):
                               print(g_module_name + "TRACE:WARN:ADDING_TO_STATE",g_num_names_added_to_state,name_with_uri,checksum_part);

                       # Regardless if we have seen this file before or not, we save its state to g_state_for_saving_dictionary for keep the state currrent.
                       g_state_for_saving_dictionary[name_with_uri] = checksum_part; # Save to g_state_for_saving_dictionary so we can keep the state current, i.e the state doesn't grow beyond today's files.
                       g_num_added_to_saving_dictionary += 1;
                       if (g_trace_flag):
                           print(g_module_name + "TRACE:ADDING_TO_SAVING_DICTIONARY",g_num_added_to_saving_dictionary,name_with_uri,checksum_part);

                    if (g_trace_flag):
                        print(g_module_name + "new_line",new_line);

                    # Parse the file name into its components: T2015001225500.L2_SNPP_SST3.nc
                    # into type, year, day of year, and hour.

                    (o_stream_type_field,o_year_field,o_day_of_year_field,o_hour_field) = parse_name_for_time_fields(filename_part);

                     # Calculate the month from the current date.

                    date_in_date_format = datetime.datetime(int(o_year_field), 1, 1) + datetime.timedelta(int(o_day_of_year_field) - 1);
                    o_month_field = '{:02d}'.format(date_in_date_format.month);

                    # Update all previous_ and current_ variables.

                    (previous_year,
                     current_year,
                     previous_month,
                     current_month,
                     previous_week,
                     current_week,
                     previous_day_of_year,
                     current_day_of_year, previous_hour,
                     current_hour) = set_appropriate_fields(search_groupby,
                                                            previous_year,
                                                            current_year,
                                                            previous_month,
                                                            current_month,
                                                            previous_week,
                                                            current_week,
                                                            previous_day_of_year,
                                                            current_day_of_year,
                                                            previous_hour,
                                                            current_hour,
                                                            o_year_field,o_month_field,o_day_of_year_field,o_hour_field);
                    if (g_trace_flag):
                        print(g_module_name + 'TRACE:previous_year', previous_year)
                        print(g_module_name + 'TRACE:current_year', current_year)
                        print(g_module_name + 'TRACE:previous_month', previous_month)
                        print(g_module_name + 'TRACE:current_month', current_month)
                        print(g_module_name + 'TRACE:previous_week', previous_week)
                        print(g_module_name + 'TRACE:current_week', current_week)
                        print(g_module_name + 'TRACE:previous_day_of_year', previous_day_of_year)
                        print(g_module_name + 'TRACE:current_day_of_year', current_day_of_year)
                        print(g_module_name + 'TRACE:previous_hour', previous_hour)
                        print(g_module_name + 'TRACE:current_hour', current_hour)

                    field_changed_flag  = detect_if_particular_field_has_changed(search_groupby,
                                                                                 previous_year,
                                                                                 current_year,
                                                                                 previous_month,
                                                                                 current_month,
                                                                                 previous_week,
                                                                                 current_week,
                                                                                 previous_day_of_year,
                                                                                 current_day_of_year,
                                                                                 previous_hour,
                                                                                 current_hour);
                    if (g_trace_flag):
                        print(g_module_name + 'TRACE:field_changed_flag',field_changed_flag,'previous_year', previous_year,'current_year', current_year,'previous_month', previous_month,'current_month', current_month,'previous_week', previous_week,'current_week', current_week,'previous_day_of_year', previous_day_of_year,'current_day_of_year', current_day_of_year,'previous_hour', previous_hour,'current_hour', current_hour)
                    
                    if (first_name_found_flag == False) or (field_changed_flag == True):
                        first_name_found_flag = True;

                        previous_output_file_name = output_file_name;

                        output_file_name = create_new_out_file_name(BASE_OUTPUT_FILE_NAME,
                                                                    search_groupby,
                                                                    previous_year,
                                                                    current_year,
                                                                    previous_month,
                                                                    current_month,
                                                                    previous_week,
                                                                    current_week,
                                                                    previous_day_of_year,
                                                                    current_day_of_year,
                                                                    previous_hour,
                                                                    current_hour)

                        # A note about the output file name when the state file is used because the user is crawling for current file:
                        #   -- It is possible the previous output filename to still be sitting in the output directory, we need to be
                        #      careful to not overwrite the existing file.  Check to see if the file exist.  If it is, we append
                        #      the output_file_name with the current time, include hour minute and seconds and the epoch time.

                        if (os.path.isfile(output_file_name)):
                             print(g_module_name + 'WARN:output_file_name',output_file_name,'exists');
                             # Create a new file name with time appended.
                             import pytz
                             pacific = pytz.timezone("US/Pacific")
                             now = datetime.datetime.now(pacific);
                             # Add the hours, minute, seconds and the epoch time to the file name so we have a different file name altogether.
                             output_file_name = output_file_name + '_created_time_' + str(now.hour).zfill(2) + '_' + str(now.minute).zfill(2) + '_' + str(now.second).zfill(2) + "_" + str(int(round(time.time() *1000)));
                             print(g_module_name + 'WARN:updated:output_file_name',output_file_name);

                        if (g_debug_flag):
                            print(g_module_name + 'output_file_name',output_file_name);

                        # If we found at least one name, we create an output file to allow subsequent looping to write to the output file.
                        # Do a sanity check to see if the file pointer is already opened, then close it.
                        if (output_file_pointer != 'DUMMY_OUTPUT_FILE_POINTER'):
                            output_file_pointer.close();                         # Close the previously open file pointer.
                            list_of_output_file_names.append(previous_output_file_name + ' ' + str(found_names));  # Save the file name and how many files saved.
                            all_names_found_in_execution += found_names;
                            found_names = 0;  # Reset this variable for the next file.

                        # Now that the name has been created, we open to write our first name.
                        output_file_pointer = open(output_file_name, 'w');
                    # end of if (first_name_found_flag == False) or (field_changed_flag == True)

                    # Before writing this file name to file, we check to see what the state of that file was.
                    # If we have seen it before, we won't write.
                    # Do the normal write since we know that the file has been created already.
                    if (file_state_status == "ready_for_saving"):  # Seen before with different checksum.
                        output_file_pointer.write(new_line + '\n');
                        found_names += 1; # Keep track of how many names we have written to this new file.
                        if (g_trace_flag):
                            print(g_module_name + "TRACE:file_state_status",file_state_status,"new_line",new_line)
                    else:
                        if (g_trace_flag):
                            print(g_module_name + "TRACE:file_state_status",file_state_status,"new_line",new_line)
                # end if (re.search(pattern_to_look_for,filename_part)):
                else:
                    g_num_names_dissimilar_pattern += 1; # Number of file names different than pattern_to_look_for object.
                    if (g_trace_flag):
                         print(g_module_name + "TRACE:NAME_DISSIMILIAR",filename_part);
                   
            else:
                if (g_debug_flag):
                    print(g_module_name + "ERROR: Line only contain 1 token [" + one_line + "]")

    if (g_debug_flag):
        print(g_module_name + "query_string [" + query_string + "]")

    # If we have written the file, close it otherwise print an ERROR.
    if (output_file_pointer != 'DUMMY_OUTPUT_FILE_POINTER'):
        output_file_pointer.close();
        list_of_output_file_names.append(output_file_name+ ' ' + str(found_names));  # Save the last file name and how many files saved.
        all_names_found_in_execution += found_names;
        print('');
        print('INFO: Created file(s):');
        print('');
        for output_name in list_of_output_file_names:
            print('     ',output_name);
        print('');
        print('INFO: all_names_found_in_execution',all_names_found_in_execution,'in_files',len(list_of_output_file_names));

        # Save the state to state file.
        if (g_use_state_flag):
            if (g_debug_flag):
                 print(g_module_name + "g_use_state_flag is True");
            # If we had added a new state or replace an existing one, re-write the names in g_state_for_saving_dictionary to same state file.
            if ((g_num_names_added_to_state > 0) or (g_num_names_replaced_in_state > 0)):
              try:
                with open(default_state_filename, "w") as text_file:
                    text_file.writelines((k + " " + v + "\n") for k, v in list(g_state_for_saving_dictionary.items()));  # Save all items from g_state_for_saving_dictionary to file so we can ignore files next time we see them.
                text_file.close();
                print(g_module_name + "INFO:NUM_NAMES_ADDED_TO_STATE                      ",g_num_names_added_to_state,"DEFAULT_STATE_FILENAME",default_state_filename);
                print(g_module_name + "INFO:NUM_NAMES_REPLACED_IN_STATE                   ",g_num_names_replaced_in_state,"DEFAULT_STATE_FILENAME",default_state_filename);
                if (default_state_filename != ""):
                    print(g_module_name + "INFO:DEFAULT_STATE_FILENAME                   ",default_state_filename);
                print(g_module_name + "INFO:NUM_NAMES_FOUND_FROM_CRAWLING            ",g_num_names_found_from_crawling);
                print(g_module_name + "INFO:NUM_EXISTING_NAMES_SAME_CHECKSUM_IN_STATE",g_num_existing_names_same_checksum_in_state);
                print(g_module_name + "INFO:NUM_EXISTING_NAMES_DIFFERENT_CHECKSUM_IN_STATE",g_num_existing_names_different_checksum_in_state);
              except:
                print(g_module_name + "ERROR:len(g_state_for_saving_dictionary)",len(g_state_for_saving_dictionary));
                print(g_module_name + "ERROR:Had issues writing content of g_state_for_saving_dictionary to state file " + default_state_filename);
                sys.exit(0);


            if (g_debug_flag):
                print(g_module_name + "g_num_existing_names_same_checksum_in_state",g_num_existing_names_same_checksum_in_state);
                print(g_module_name + "g_num_existing_names_different_checksum_in_state",g_num_existing_names_different_checksum_in_state);
                print(g_module_name + "g_num_names_found_from_crawling",g_num_names_found_from_crawling);
                print(g_module_name + "g_num_names_matching_pattern",g_num_names_matching_pattern);
                print(g_module_name + "g_num_names_dissimilar_pattern",g_num_names_dissimilar_pattern);
                print(g_module_name + "search_current_only_value",search_current_only_value);
                print(g_module_name + "loaded default_state_filename",default_state_filename,"g_num_file_states_loaded",g_num_file_states_loaded,"len(g_state_dictionary)",len(g_state_dictionary));
        else:
            if (g_debug_flag):
                print("g_use_state_flag is False");

    else:
        if (default_state_filename != ""):
            print(g_module_name + "INFO:DEFAULT_STATE_FILENAME                   ",default_state_filename);
        print(g_module_name + "INFO:NUM_NAMES_FOUND_FROM_CRAWLING            ",g_num_names_found_from_crawling);
        print(g_module_name + "INFO:NUM_EXISTING_NAMES_SAME_CHECKSUM_IN_STATE",g_num_existing_names_same_checksum_in_state);
        print(g_module_name + "INFO:No new files were found from the following query_string")
        print(query_string);
        print("curl_command command: curl \"" + query_string + "\"");
        # Add a few more debug prints so the user know why zero files are returned.
        print("regular_expression_to_check [", regular_expression_to_check, "]");
        print("CRAWLER_SEARCH_FILE_PATTERN[",os.getenv('CRAWLER_SEARCH_FILE_PATTERN',''),"]");

    return(o_encountered_error_flag);

# Function updates all the fields after the fields have been parsed from the file name.
def set_appropriate_fields(i_search_groupby,
                           i_previous_year,
                           i_current_year,
                           i_previous_month,
                           i_current_month,
                           i_previous_week,
                           i_current_week,
                           i_previous_day_of_year,
                           i_current_day_of_year,
                           i_previous_hour,
                           i_current_hour,
                           i_year_field,i_month_field,i_day_of_year_field,i_hour_field):

    o_previous_year  = i_previous_year;
    o_current_year   = i_current_year;
    o_previous_month = i_previous_month;
    o_current_month  = i_current_month;
    o_previous_week  = i_previous_week;
    o_current_week   = i_current_week;
    o_previous_day_of_year = i_previous_day_of_year;
    o_current_day_of_year  = i_current_day_of_year;
    o_previous_hour        = i_previous_hour;
    o_current_hour         = i_current_hour;

    # Set the year variables.

    o_previous_year = i_current_year;
    o_current_year  = i_year_field;

    # Calculate the month variables.

    o_previous_month = i_current_month;
    o_current_month  = i_month_field;

    # Calulate and set the week variables.
    week = convert_from_day_of_year_to_week(i_day_of_year_field);

    o_previous_week = i_current_week;
    o_current_week  = str(week);

    # Set the day of year variables.
    o_previous_day_of_year = i_current_day_of_year;
    o_current_day_of_year  = i_day_of_year_field;

    # Set the hour variables.

    o_previous_hour = i_current_hour;
    o_current_hour  = i_hour_field;

    return(o_previous_year,
           o_current_year,
           o_previous_month,
           o_current_month,
           o_previous_week,
           o_current_week,
           o_previous_day_of_year,
           o_current_day_of_year,
           o_previous_hour,
           o_current_hour)

# Function create a new output name given how the grouping is requested, using all the previous and current date fields.
def create_new_out_file_name(i_output_file_name,
                             i_search_groupby,
                             i_previous_year,
                             i_current_year,
                             i_previous_month,
                             i_current_month,
                             i_previous_week,
                             i_current_week,
                             i_previous_day_of_year,
                             i_current_day_of_year,
                             i_previous_hour,
                             i_current_hour):


    # Also calculate the current day of month
    date_in_date_format = datetime.datetime(int(i_current_year), 1, 1) + datetime.timedelta(int(i_current_day_of_year) - 1);
    day_of_month = '{:02d}'.format(date_in_date_format.day);

    if i_search_groupby == 'yearly':  # viirs_filelist.txt.yearly_2014
        o_output_file_name = i_output_file_name + '.' + i_search_groupby + '_' + str(i_current_year); 
    if i_search_groupby == 'monthly': # viirs_filelist.txt.monthly_2014_01
        o_output_file_name = i_output_file_name + '.' + i_search_groupby + '_' + str(i_current_year) + '_' + i_current_month; 
    if i_search_groupby == 'weekly':  # viirs_filelist.txt.weekly_2014_01
        o_output_file_name = i_output_file_name + '.' + i_search_groupby + '_' + str(i_current_year) + '_' + i_current_week                                    + '_date_' + str(i_current_year) + '_' + str(i_current_month) + '_' + str(day_of_month); 
    if i_search_groupby == 'daily':   # viirs_filelist.txt.daily_2014_001
        o_output_file_name = i_output_file_name + '.' + i_search_groupby + '_' + str(i_current_year) + '_' + i_current_day_of_year                             + '_date_' + str(i_current_year) + '_' + str(i_current_month) + '_' + str(day_of_month); 
    if i_search_groupby == 'hourly': # viirs_filelist.txt.hourly_2014_001_20
        o_output_file_name = i_output_file_name + '.' + i_search_groupby + '_' + str(i_current_year) + '_' + i_current_day_of_year + '_' + str(i_current_hour) + '_date_' + str(i_current_year) + '_' + str(i_current_month) + '_' + str(day_of_month); 
#        exit(0);
    
    return(o_output_file_name);

# Function convert a day of year to week of year.
def convert_from_day_of_year_to_week(i_day_of_year):

    (quotient, remainder) = divmod(int(i_day_of_year), 7);

    # Example of logic:
    #
    # 5  divide by 7 has remainder as 5, quotient as 0, thus its week is quotient plus 1 (0 + 1)
    # 6  divide by 7 has remainder as 6, quotient as 0, thus its week is quotient plus 1 (0 + 1)
    # 7  divide by 7 has remainder as 0, quotient as 1, thus its week is quotient        (1)
    # 8  divide by 7 has remainder as 1, quotient as 1, thus its week is quotient plus 1 (1 + 1)
    # 9  divide by 7 has remainder as 2, quotient as 1, thus its week is quotient plus 1 (1 + 1)
    # 10 divide by 7 has remainder as 3, quotient as 1, thus its week is quotient plus 1 (1 + 1)
    # 11 divide by 7 has remainder as 4, quotient as 1, thus its week is quotient plus 1 (1 + 1)
    # 12 divide by 7 has remainder as 5, quotient as 1, thus its week is quotient plus 1 (1 + 1)
    # 13 divide by 7 has remainder as 6, quotient as 1, thus its week is quotient plus 1 (1 + 1)
    # 14 divide by 7 has remainder as 0, quotient as 2, thus its week is quotient        (2)
    # 15 divide by 7 has remainder as 1, quotient as 2, thus its week is quotient plus 1 (2 + 1)

    # If there is no remainder, the week is the same as the quotient.  If the remainder is greater than 0, the week is quotient plus 1.
    if (remainder == 0):
        the_week = quotient;
    else:
        the_week = quotient + 1;

    # Prepend with 1 leading zero for the week since we have 52 weeks total in a year.

    o_week = "%02d" % (the_week,);

    return(o_week);

# Function detects if a particular field has changed from one file name to the next. This will allow the parent 
def detect_if_particular_field_has_changed(i_search_groupby,
                                           i_previous_year,
                                           i_current_year,
                                           i_previous_month,
                                           i_current_month,
                                           i_previous_week,
                                           i_current_week,
                                           i_previous_day_of_year,
                                           i_current_day_of_year,
                                           i_previous_hour,
                                           i_current_hour):

    o_field_changed_flag = False;

    if i_search_groupby == 'yearly':
        if i_previous_year != i_current_year:
            o_field_changed_flag = True;
    if i_search_groupby == 'monthly':
        if i_previous_month != i_current_month:
            o_field_changed_flag = True;
    if i_search_groupby == 'weekly':
        if i_previous_week != i_current_week:
            o_field_changed_flag = True;
        # If the year changed but the week remains the same, the field has changed.
        if i_previous_year != i_current_year:
            o_field_changed_flag = True;
    if i_search_groupby == 'daily':
        if i_previous_day_of_year != i_current_day_of_year:
            o_field_changed_flag = True;
        # If the year changed but the week remains the same, the field has changed.
        if i_previous_year != i_current_year:
            o_field_changed_flag = True;
    if i_search_groupby == 'hourly':
        # If the year changed, then field has changed.
        if i_previous_year != i_current_year:
            o_field_changed_flag = True;
        # If the day of year changed, then field has changed.
        if i_previous_day_of_year != i_current_day_of_year:
            o_field_changed_flag = True;
        # If the hour has changed, then field indeed has changed.
        if i_previous_hour != i_current_hour:
            o_field_changed_flag = True;

    return(o_field_changed_flag);

def parse_name_for_time_fields(i_filename):
    # Parse the file name into its components: T2015001225500.L2_SNPP_SST3.nc
    #                                          A2016234.L3m_DAY_SST_sst_9km.nc
    #                                          0123456789012345678901234567890123456789
    # into type, year, day of year, and hour.
 
    debug_module = 'parse_name_for_time_fields:';

    o_stream_type_field = i_filename[0:1];
    o_year_field        = i_filename[1:5];
    o_day_of_year_field = i_filename[5:8];

    potential_hour_portion = i_filename[8:10];

    if i_filename.startswith('AQUA_MODIS') or i_filename.startswith('TERRA_MODIS') or \
       i_filename.startswith('SNPP_VIIRS'):
           # It is awkward to parse for the o_stream_type_field because VIIRS does not start with 'V'
           if i_filename.startswith('AQUA_MODIS') or i_filename.startswith('AQUA_TERRA'):
               o_stream_type_field = i_filename[0:1];
           if i_filename.startswith('SNPP_VIIRS'): 
               o_stream_type_field = 'V';
           # Split the file name so we can get to the time field SNPP_VIIRS.2019176T235400.L2.SST.nc
           name_fields_array = i_filename.split(".");
           # The name fields is the 2nd token SNPP_VIIRS.20190625T235400.L2.SST.nc, which is 20190625T235400
           #                                  0   4   9
           o_year_field           = name_fields_array[1][0:4]; # Second token is index 1.
           month_field            = name_fields_array[1][4:6]; # Second token is index 1 
           day_field              = name_fields_array[1][6:8]; # Second token is index 1 
           mydate = datetime.date(int(o_year_field),int(month_field),int(day_field)); 
           o_day_of_year_field = mydate.strftime("%j");
           #print(mydate.strftime("%A"))
           #print(mydate.strftime("%j"))
           #print(debug_module + "o_year_field ",o_year_field)
           #print(debug_module + "month_field",month_field)
           #print(debug_module + "day_field",day_field)
           #print(debug_module + "i_filename",i_filename)
           #print(debug_module + "o_year_field ",o_year_field)
           #print(debug_module + "o_day_of_year_field",o_day_of_year_field)
           #print(debug_module + "This is a bug.")
           #exit(0);
           potential_hour_portion = name_fields_array[1][9:11];# Second token is index 1.

    # Note: Some granules may not have hour field in the name (particularly those that are daily).
    #       So we do some checking.  If it contains non digit, we set it to some default, perhaps "00";
    #potential_hour_portion = i_filename[8:10];
    if (potential_hour_portion.isalnum()):
        o_hour_field = potential_hour_portion;
    else:
        o_hour_field = "00";
        if (g_trace_flag):
            print(debug_module + "TRACE:WARN: File does not contain hour portion" + i_filename);

    #exit(0);

    return(o_stream_type_field,o_year_field,o_day_of_year_field,o_hour_field);

def validate_search_filter(i_search_filter):
    # Do a sanity check on the search_filter to see that it at least contain the year
    # For some strange reason, the search query behaves badly when you don't give it a year, i.e.
    # it will not give you any result and you will be frustrated wondering why this thing won't work.
    # If filte is bad, we print an error message and exit to give the user a chance to correct.
    
    year_portion = "";

    try:
        # A search_filter should look something like these and should at least contain the year after the first character:
        #
        #     A2016001*
        #     A201601*
        #     A20160*
        #     A2016*
        #
        #     01234567890
        year_portion = i_search_filter[1:5];
        datetime.datetime.strptime(year_portion, '%Y');

        # Check to see that it ends with '*'
        if (not i_search_filter.endswith("*")):
            print("ERROR: The i_search_filter should end with '*'");
            print("ERROR: Specified i_search_filter =",i_search_filter);
            sys.exit(0);
    except ValueError:
        print("i_search_filter",i_search_filter);
        print("year_portion ",year_portion);
        print("ERROR: The i_search_filter should at least contain the year");
        sys.exit(0);
#        raise ValueError("Incorrect data format for search_filter field, should be YYYY: " + search_filter);

if __name__ == "__main__":
   main(sys.argv[1:])
