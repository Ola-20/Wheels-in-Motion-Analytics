
FROM apache/airflow:2.10.2

# Install wget at the OS level
USER root
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Use the non-root 'airflow' user (inside Airflow's venv)
USER airflow

# Requirements
COPY --chown=airflow:root requirements.txt /requirements.txt

# Airflow version to match constraints
ARG AIRFLOW_VERSION=2.10.2

# Compute Python MAJOR.MINOR inside the image, then use it for constraints
RUN PYVER=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') \
 && pip install --no-cache-dir -r /requirements.txt \
    -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYVER}.txt"

WORKDIR /opt/airflow