FROM apache/airflow:2.7.0-python3.10

# Install extra packages (airflow is already in base image)
RUN pip install kafka-python pandas requests matplotlib psycopg2-binary

# Set path for imports
ENV PYTHONPATH="/opt/airflow/src"
