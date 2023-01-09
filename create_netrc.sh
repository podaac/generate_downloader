#! /bin/bash

# Requires following environment variables to be defined:
#     MACHINE
#     LOGIN
#     PASSWORD
#
# Typically passed in via --build-arg arguments to docker build command.

# Create .netrc file and set permissions
netrc=/root/.netrc
touch $netrc
chmod 644 $netrc

# Print required data from environment to file
printf "machine %s\n" $1 >> $netrc
printf "login %s\n" $2 >> $netrc
printf "password %s\n" $3 >> $netrc