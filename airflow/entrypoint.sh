#!/bin/bash

# Initialize DB
airflow db init

# Create admin user if it doesn't exist
airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}" || true

# Start scheduler in background
airflow scheduler &

# Start webserver (PID 1)
exec airflow webserver
