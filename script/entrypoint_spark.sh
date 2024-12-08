#!/bin/bash
set -e

echo "Entrypoint script started."

# Install dependencies from requirements.txt
if [ -f "/opt/airflow/requirements-spark.txt" ]; then
  echo "Installing dependencies from requirements-spark.txt..."
  python3 -m pip install --upgrade pip
  python3 -m pip install --user --no-cache-dir -r /opt/airflow/requirements-spark.txt
else
  echo "requirements-spark.txt not found. Skipping dependency installation."
fi

echo "Dependencies installed successfully."

# Install and set up cqlsh
if ! command -v cqlsh &> /dev/null; then
  echo "Installing cqlsh..."
  apt-get update
  apt-get install -y python3 wget
  wget https://downloads.apache.org/cassandra/tools/4.0.6/apache-cassandra-4.0.6-bin.tar.gz
  tar -xvf apache-cassandra-4.0.6-bin.tar.gz
  mv apache-cassandra-4.0.6/tools/bin/cqlsh /usr/local/bin/
  chmod +x /usr/local/bin/cqlsh
  rm -rf apache-cassandra-4.0.6*
fi

# Set up Cassandra keyspace and table
if command -v cqlsh &> /dev/null; then
  echo "Setting up Cassandra keyspace and table..."
  cqlsh cassandra -e "
    CREATE KEYSPACE IF NOT EXISTS spark_streams
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT
    );
  "
  echo "Cassandra setup complete."
else
  echo "cqlsh command not found. Skipping Cassandra keyspace and table setup."
fi

echo "Entrypoint script execution complete."

exec "$@"
