#  Copyright 2017, by the California Institute of Technology.  ALL RIGHTS
#  RESERVED. United States Government Sponsorship acknowledged. Any commercial
#  use must be negotiated with the Office of Technology Transfer at the
#  California Institute of Technology.
#
# $Id$
# DO NOT EDIT THE LINE ABOVE - IT IS AUTOMATICALLY GENERATED BY CM
#
# This function will perfom a check on the required settings for the downloader.
# If a required setting is missing, it will exit to give the operator a change to do the setting.

import os
import sys

from log_this import log_this;

def inspect_required_env_settings():
    debug_module = "inspect_required_env_settings:";
    o_all_required_settings_are_ok = 1;

    required_variable = "SIGEVENT_SOURCE";
    if (os.getenv(required_variable,"") == ""):
        log_this("ERROR",debug_module,"Required setting " + required_variable + " is missing.  Program terminating.");
        o_all_required_settings_are_ok = 0;
        sys.exit(1);

    required_variable = "SCRATCH_AREA";
    if (os.getenv(required_variable,"") == ""):
        log_this("ERROR",debug_module,"Required setting " + required_variable + " is missing.  Program terminating.");
        o_all_required_settings_are_ok = 0;
        sys.exit(1);

    # If we got to here, all the required settings are OK.

    return(o_all_required_settings_are_ok);

if __name__ == "__main__":
    all_required_settings_are_ok = inspect_required_env_settings();
    print("all_required_settings_are_ok",all_required_settings_are_ok)
    sys.exit(0);
