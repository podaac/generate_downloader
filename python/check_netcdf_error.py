#!/app/env/bin/python3
"""Script to read in log and determine if there has been an error with the 
download of a NetCDF file making it corrupted.

Prints "error" for communication with calling script.
"""

# Standard imports
import sys

def read_log(log_file):
    with open(log_file) as f:
        logs = f.readlines()
    
    found_error = False
    for line in logs:
        if "[Errno -101] NetCDF: HDF error:" in line:
            found_error = True
            break
    
    if found_error:
        print("error")
    else:
        print("none")
    
if __name__ == "__main__":
    log_file = sys.argv[1]
    read_log(log_file)