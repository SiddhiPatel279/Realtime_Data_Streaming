import csv
import uuid
import json
import time
import os
from confluent_kafka import Producer
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 11, 15, 10, 00)
}

def get_data(csv_file_path):
    """
    Reads the CSV file row by row and returns one row at a time.
    """
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found at {csv_file_path}")

    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        for row in csv_reader:
            yield row  # Yield each row one by one

def format_data(row):
    """
    Formats the row data and adds additional details like UUID.
    """
    data = {
        'id': str(uuid.uuid4()),
        'date': row['Date'],
        'time': row['Time'],
        'global_active_power': row['Global_active_power'],
        'global_reactive_power': row['Global_reactive_power'],
        'voltage': row['Voltage'],
        'global_intensity': row['Global_intensity'],
        'sub_metering_1': row['Sub_metering_1'],
        'sub_metering_2': row['Sub_metering_2'],
        'sub_metering_3': row['Sub_metering_3']
    }
    return data

def delivery_report(err, msg):
    """
    A callback function for the Kafka producer to log the delivery result.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def stream_data(csv_file_path):
    """
    Streams formatted data to a Kafka topic with a 1-second delay between each row.
    Uses Confluent Kafka Producer.
    """
    conf = {'bootstrap.servers': 'broker:29092'}
    producer = Producer(conf)

    for row in get_data(csv_file_path):
        formatted_data = format_data(row)
        json_data = json.dumps(formatted_data).encode('utf-8')

        try:
            producer.produce('energyConsumptionRecords', json_data, callback=delivery_report)
        except Exception as e:
            print(f"Failed to produce message: {e}")

        producer.poll(0)
        time.sleep(1)  # Wait 1 second before sending the next message

    producer.flush()

csv_file_path = './data/household_power_consumption.csv'

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_csv_to_kafka',
        python_callable=stream_data,
        op_kwargs={'csv_file_path': csv_file_path}
    )
