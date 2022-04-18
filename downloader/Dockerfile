# Stage 0 - Create from Python 3.10-alpine3.15 image
# FROM python:3.10-alpine3.15 as stage0
FROM python:3.10-alpine3.15
RUN apk add --no-cache --upgrade bash tcsh python3-dev linux-headers gcc musl-dev

# Stage 1 - Copy Generate code
# FROM stage0 as stage1
RUN /bin/mkdir /data
COPY . /app

# Stage 2 - Install dependencies
# FROM stage2 as stage3
RUN /usr/local/bin/python3 -m venv /app/env
RUN /app/env/bin/pip install -r /app/requirements.txt

# Stage 3 - .netrc credentials
# FROM stage2 as stage3
ARG MACHINE
ARG LOGIN
ARG PASSWORD
RUN /bin/chmod +x /app/create_netrc.sh
RUN /app/create_netrc.sh $MACHINE $LOGIN $PASSWORD
RUN /bin/rm /app/create_netrc.sh

# Stage 4 - Execute code
# FROM stage3 as stage4
LABEL version="0.1" \
    description="Containerized Generate: Downloader"
ENTRYPOINT [ "/bin/tcsh", "/app/shell/startup_generic_downloader.csh" ] 