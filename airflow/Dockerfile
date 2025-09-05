# Use an Airflow version compatible with google provider >=10.x
FROM apache/airflow:2.10.2

ENV AIRFLOW_HOME=/opt/airflow

USER root

# (No browser needed; keep image lean)
# If you later add OS deps (e.g., for other DAGs), do it here.

# Install Python deps
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR ${AIRFLOW_HOME}

# Back to airflow user (Airflow images define this)
USER ${AIRFLOW_UID}
