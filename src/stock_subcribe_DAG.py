import datetime
from google.cloud import bigquery
import json
from json import loads, dumps
from fastavro import writer, parse_schema, reader
from google.cloud import pubsub_v1
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from vnstock import *
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago



default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),  # Set the execution timeout to 30 minutes
    # Other default arguments
}

# Create a DAG instance
dag = DAG(
    'publish_stock_data_to_pubsub',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval or None
    start_date=days_ago(2),
    catchup=False,
    
)

# Get the current timestamp
current_time = datetime.now()

# Define a function to publish stock data to Pub/Sub
def stock_subscribe_pubsub():
    # Initialize a PublisherClient for Pub/Sub
    publisher = pubsub_v1.PublisherClient()

    # Define the path to the Pub/Sub topic (replace with actual topic path)
    topic_path = "projects/pyspark-practice-395516/topics/stock_subcribe_data"

    # Get stock data using the 'price_depth' function for specific stocks
    stock_data = price_depth('TCB,SSI')[['Mã CP', 'Giá tham chiếu', 'Giá khớp lệnh', 'KL Khớp lệnh']]
    stock_data = stock_data.rename(columns={
    'Mã CP': 'Ma_CP',
    'Giá tham chiếu': 'Gia_tham_chieu',
    'Giá khớp lệnh': 'Gia_khop_lenh',
    'KL Khớp lệnh': 'KL_Khop_lenh'
    })
    # Add a 'Current Time' column with the current timestamp to stock data
    stock_data['Current Time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')

    # Iterate through each row in stock data
    for index, value in stock_data.iterrows():
        # Convert the row to a dictionary
        data_dict = value.to_dict()

        # Convert the dictionary to a JSON string
        message = json.dumps(data_dict, ensure_ascii = False)

        # Encode the message string to utf-8
        message_str = message.encode('utf-8')

        # Publish the message to the specified Pub/Sub topic
        publish = publisher.publish(topic_path, message_str)


publish_stock_data_task = PythonOperator(
    task_id='publish_stock_data',
    python_callable=stock_subscribe_pubsub,
    dag=dag,
)



publish_stock_data_task