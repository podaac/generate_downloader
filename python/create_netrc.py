#!/usr/local/bin/python
#
#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# Grabs EDL username and password from Parameter Store and creates a .netrc file
# in order to provide download access to OBPG files.
#

# Standard imports
import os
import pathlib

# Third-party imports
import boto3
import botocore

# Constants
URS = "urs.earthdata.nasa.gov"
NETRC_DIR = pathlib.Path(os.getenv("NETRC_DIR"))

def create_netrc():
    """Writes a .netrc file at NETRC_DIR environment variable path.
    
    Raises an exception if can't access SSM client.
    """
    
    # Get Earthdata Login username and password
    try:
        ssm_client = boto3.client('ssm', region_name="us-west-2")
        username = ssm_client.get_parameter(Name="generate-edl-username", WithDecryption=True)["Parameter"]["Value"]
        password = ssm_client.get_parameter(Name="generate-edl-password", WithDecryption=True)["Parameter"]["Value"]
    except botocore.exceptions.ClientError as error:
        raise error
    
    # Create netrc file
    with open(NETRC_DIR.joinpath(".netrc"), 'w') as fh:
        fh.write(f"machine {URS}\n")
        fh.write(f"login {username}\n")
        fh.write(f"password {password}\n")   

    print(f"Created .netrc file at : {str(NETRC_DIR)}")
    
def remove_netrc():
    """Remove .netrc created file."""
    
    os.remove(NETRC_DIR.joinpath(".netrc"))
    print("Remove .netrc file")