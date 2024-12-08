import json
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import logging
default_args = {
    'owner': 'houssem',
    'start_date': datetime(2024, 12, 7, 10, 00)
}

def on_send_success(record_metadata):
    logging.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition}")

def on_send_error(excp):
    logging.error(f"Error sending message: {excp}")

def submit_spark_job():
    command = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,com.datastax.spark:spark-cassandra-connector_2.13:3.4.1",
        "/opt/airflow/spark_stream.py"
    ]
    subprocess.run(command, check=True)

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())  # Convert UUID to string
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        logging.info("Starting the stream_data task")
        try:
            res = get_data()
            res = format_data(res)
            logging.info(f"Fetched data: {res}")
            producer.send('users_created_new_topic', json.dumps(res).encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
            producer.flush()
            logging.info(f"Fetched producer: {producer}")
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('streaming_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    kafka_streaming = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    spark_job = PythonOperator(
        task_id='submit_spark_job',
        python_callable=submit_spark_job
    )

    kafka_streaming >> spark_job
