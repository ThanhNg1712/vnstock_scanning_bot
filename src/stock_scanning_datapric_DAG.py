from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from airflow.exceptions import AirflowException
import subprocess
from airflow.operators.python_operator import PythonOperator




default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),  # Set the execution timeout to 30 minutes
    # Other default arguments
}

CLUSTER_NAME = 'stablestock-cluster-auto'
REGION = 'us-central1'
PROJECT_ID = 'pyspark-practice-395516'
PYSPARK_URI = 'gs://vn_stock_project/stock_scan_dataproc.py'
#PYTHON_SCRIPT_URI = 'gs://vn_stock_project/vn_stock_all.py'
#DAY_PYTHON_SCRIPT_URI = 'gs://vn_stock_project/vnstock_day2.py'

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    }
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

dag = DAG(
    'stock_scanning_final',
    default_args=default_args,
    description='Stock_Scanning-Pipeline',
    schedule_interval='0 10 * * *',  # Set the schedule to run daily at 10 a.m.,
    start_date=days_ago(2)
)

create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        dag=dag,
    )

submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION,  # Use 'region' instead of 'location'
        project_id=PROJECT_ID,
        dag=dag,
    )

delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION,
        dag=dag,
    )

    # Define task dependencies
create_cluster >> submit_job >> delete_cluster