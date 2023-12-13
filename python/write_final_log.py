
#
# Function to write a message to the final log file which will later be 
# retrieved and logged to support Cloud Metrics.
#

# Standard imports
import os

def write_final_log(message):
    """Write message to final log file."""

    final_log = os.environ.get("FINAL_LOG_MESSAGE")
    with open(final_log, 'a') as fh:
        fh.write(f"{message}\n") 