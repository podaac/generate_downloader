#!/app/env/bin/python3
"""Logs final log statement.

Logs a final log statement from a TXT file used by the downloader to track data
on current state for Cloud Metrics.
"""

# Standard imports
import logging
import os
import pathlib
import sys

def print_final_log():
    """Print final log message."""
    
    logger = get_logger()
    
    # Open file used to track data
    log_file = pathlib.Path(os.getenv('FINAL_LOG_MESSAGE'))
    with open(log_file) as fh:
        log_data = fh.read().splitlines()

    # Organize file data into a string
    execution_data = ""
    processed = []
    failed_downloads = []
    for line in log_data:
        if "execution_data" in line: execution_data += line.split("execution_data: ")[-1]
        if "processed" in line: processed.append(line.split("processed: ")[-1])
        if "number_downloads" in line: execution_data += f" - {line}"
        if "failed_download" in line and "number_failed_downloads" not in line: failed_downloads.append(line.split("failed_download: ")[-1])
        if "number_failed_downloads" in line: execution_data += f" - {line}"
    
    final_log_message = ""
    if execution_data: final_log_message += f"{execution_data} - "
    if len(processed) > 0: final_log_message += f"processed: {', '.join(processed)} - "
    if len(failed_downloads) > 0: final_log_message += f"failed_downloads: {', '.join(failed_downloads)} - "
    
    # Print final log message and remove temp log file
    logger.info(final_log_message)
    # log_file.unlink()
    
def get_logger():
    """Return a formatted logger object."""
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        # Create a handler to console and set level
        console_handler = logging.StreamHandler(sys.stdout)    # Log to standard out to support IDL SPAWN

        # Create a formatter and add it to the handler
        console_format = logging.Formatter("%(module)s - %(levelname)s : %(message)s")
        console_handler.setFormatter(console_format)

        # Add handlers to logger
        logger.addHandler(console_handler)

    # Return logger
    return logger
    
if __name__ == "__main__":
        print_final_log()