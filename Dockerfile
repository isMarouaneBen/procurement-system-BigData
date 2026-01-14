FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libkrb5-dev \
    libsasl2-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ensure airflow is in the PATH
ENV PATH="/home/airflow/.local/bin:$PATH"