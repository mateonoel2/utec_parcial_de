#!/bin/bash

# Wait for Airflow webserver to be ready
until airflow db check; do
  echo "Waiting for Airflow database..."
  sleep 1
done

# Set Airflow variables
airflow variables set live_score_api_key "${LIVE_SCORE_API_KEY}"
airflow variables set live_score_api_secret "${LIVE_SCORE_API_SECRET}"

echo "Airflow variables have been set successfully" 