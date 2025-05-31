FROM apache/airflow:3.0.0

# Install system dependencies
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy environment variables script
COPY scripts/set_airflow_variables.sh /opt/airflow/scripts/
USER root
RUN chmod +x /opt/airflow/scripts/set_airflow_variables.sh
USER airflow