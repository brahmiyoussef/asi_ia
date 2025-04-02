from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
from models.database import connect_to_db, disconnect_from_db, sql_to_dataframe
from models.processing_pipelines import cleaning, hourly_data_per_route, time_section
from models.prompts import ollama_connect, pattern_recognition, anomaly_detection
from models.structures import PatternOutput, AnomalyAnalysisOutput

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pattern_recognition_dag',
    default_args=default_args,
    description='A DAG for pattern recognition and anomaly detection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

virtualenv_path = "/opt/airflow/dags/airflow_env/Scripts/python.exe" 

def fetch_and_clean_data(**kwargs):
    try:
        connect_to_db()
        df = sql_to_dataframe("SELECT * FROM your_table")
        cleaned_df = cleaning(df)
        kwargs['ti'].xcom_push(key='cleaned_df', value=cleaned_df)
    except Exception as e:
        logging.error(f"Error fetching and cleaning data: {e}")
        raise
    finally:
        disconnect_from_db()

def process_data(**kwargs):
    try:
        cleaned_df = kwargs['ti'].xcom_pull(key='cleaned_df', task_ids='fetch_and_clean_data')
        hourly_data = hourly_data_per_route(cleaned_df)
        time_sections = time_section(hourly_data)
        kwargs['ti'].xcom_push(key='time_sections', value=time_sections)
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

def recognize_patterns(**kwargs):
    try:
        time_sections = kwargs['ti'].xcom_pull(key='time_sections', task_ids='process_data')
        ollama = ollama_connect()
        patterns = []
        for section in time_sections:
            pattern = pattern_recognition(ollama, section)
            patterns.append(PatternOutput(pattern))
        kwargs['ti'].xcom_push(key='patterns', value=patterns)
    except Exception as e:
        logging.error(f"Error recognizing patterns: {e}")
        raise

def detect_anomalies(**kwargs):
    try:
        patterns = kwargs['ti'].xcom_pull(key='patterns', task_ids='recognize_patterns')
        anomalies = []
        for pattern in patterns:
            anomaly = anomaly_detection(pattern)
            anomalies.append(AnomalyAnalysisOutput(anomaly))
        kwargs['ti'].xcom_push(key='anomalies', value=anomalies)
    except Exception as e:
        logging.error(f"Error detecting anomalies: {e}")
        raise

fetch_and_clean_data_task = PythonOperator(
    task_id='fetch_and_clean_data',
    python_callable=fetch_and_clean_data,
    provide_context=True,
    dag=dag,
    executor_config={
        "virtualenv": {
            "python": virtualenv_path,
        },
    },
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
    executor_config={
        "virtualenv": {
            "python": virtualenv_path,
        },
    },
)

recognize_patterns_task = PythonOperator(
    task_id='recognize_patterns',
    python_callable=recognize_patterns,
    provide_context=True,
    dag=dag,
    executor_config={
        "virtualenv": {
            "python": virtualenv_path,
        },
    },
)

detect_anomalies_task = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_anomalies,
    provide_context=True,
    dag=dag,
    executor_config={
        "virtualenv": {
            "python": virtualenv_path,
        },
    },
)

fetch_and_clean_data_task >> process_data_task >> recognize_patterns_task >> detect_anomalies_task
