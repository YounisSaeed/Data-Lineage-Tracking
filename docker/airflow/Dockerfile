FROM apache/airflow:2.8.0-python3.9

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/airflow/requirements.txt /requirements.txt

RUN pip install --user --upgrade pip && \
    pip install --no-cache-dir --user -r /requirements.txt