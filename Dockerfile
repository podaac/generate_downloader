# Base OS Patching & Dependencies
FROM python:3.12-slim-bookworm

# 1. Update, upgrade, install dependencies, and clean cache in one layer
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    bash tcsh python3-venv build-essential netcdf-bin procps jq && \
    rm -rf /var/lib/apt/lists/*

# 2. Copy Generate code
RUN /bin/mkdir /data
COPY . /app

# 3. Install Python dependencies & Purge Build Tools
RUN /usr/local/bin/python3 -m venv /app/env && \
    /app/env/bin/pip install --no-cache-dir -r /app/requirements.txt && \
    apt-get purge -y build-essential && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

LABEL version="0.1" \
      description="Containerized Generate: Downloader"

ENTRYPOINT [ "/bin/tcsh", "/app/shell/startup_generic_downloader_job_index.csh" ]