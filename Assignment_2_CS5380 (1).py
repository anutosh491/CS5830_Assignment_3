#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import random
import shutil
import urllib.parse
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import apache_beam as beam
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.param import Param

# Constants for data fetching DAG
FETCH_BASE_URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
FETCH_YEAR = 2002
FETCH_NUM_FILES = 2
FETCH_ARCHIVE_OUTPUT_DIR = '/tmp/archives'
FETCH_DATA_FILE_OUTPUT_DIR = f'/tmp/data/{FETCH_YEAR}/'
FETCH_HTML_FILE_SAVE_DIR = '/tmp/html/'

# Constants for analytics DAG
ANALYTICS_ARCHIVE_PATH = "/tmp/archives/2002.zip"
ANALYTICS_REQUIRED_FIELDS = "WindSpeed, BulbTemperature"

# Default arguments for DAGs
default_fetch_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

default_analytics_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG for fetching data
dag_fetch = DAG(
    dag_id="Fetch_NCEI_Data",
    default_args=default_fetch_args,
    description='DataFetch Pipeline',
    params={
        'base_url': FETCH_BASE_URL,
        'year': Param(FETCH_YEAR, type="integer", minimum=1901, maximum=2024),
        'num_files': FETCH_NUM_FILES,
        'archive_output_dir': FETCH_ARCHIVE_OUTPUT_DIR,
    },
)

# Task 1: Download HTML page
fetch_page_task = BashOperator(
    task_id="Download_HTML_data",
    bash_command="curl {{params.base_url}}{{params.year}}/ --create-dirs -o " + FETCH_HTML_FILE_SAVE_DIR + "{{params.year}}.html",
    params={'base_url': "{{ dag_run.conf.get('base_url', params.base_url) }}"},
    dag=dag_fetch,
)

# Task 2: Select random data files
def select_random_files(num_files, base_url, year, **kwargs):
    file_save_dir = FETCH_HTML_FILE_SAVE_DIR
    with open(f"{file_save_dir}{year}.html", "r") as f:
        page_content = f.read()
    soup = BeautifulSoup(page_content, 'html.parser')
    file_links = [link.get('href') for link in soup.find_all('a') if ".csv" in link.get('href')]
    selected_files = random.sample(file_links, min(int(num_files), len(file_links)))
    return [f"{base_url}{year}/{file}" for file in selected_files]

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    op_kwargs={'num_files': "{{ dag_run.conf.get('num_files', params.num_files) }}",
               'year': "{{ dag_run.conf.get('year', params.year) }}",
               'base_url': "{{ dag_run.conf.get('base_url', params.base_url) }}"},
    dag=dag_fetch,
)

# Task 3: Download files
def download_files(selected_files, **kwargs):
    csv_output_dir = FETCH_DATA_FILE_OUTPUT_DIR
    os.makedirs(csv_output_dir, exist_ok=True)
    for file_url in selected_files:
        file_name = urllib.parse.unquote(os.path.basename(file_url))
        os.system(f"curl {file_url} -o {os.path.join(csv_output_dir, file_name)}")

fetch_files_task = PythonOperator(
    task_id='fetch_files',
    python_callable=download_files,
    provide_context=True,
    op_kwargs={'csv_output_dir': FETCH_DATA_FILE_OUTPUT_DIR},
    dag=dag_fetch,
)

# Task 4: Zip files
zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=lambda output_dir, archive_path: shutil.make_archive(archive_path, 'zip', output_dir),
    op_kwargs={'output_dir': FETCH_DATA_FILE_OUTPUT_DIR,
               'archive_path': FETCH_DATA_FILE_OUTPUT_DIR.rstrip('/')},
    dag=dag_fetch,
)

# Task 5: Move archive
move_archive_task = PythonOperator(
    task_id='move_archive',
    python_callable=lambda archive_path, target_location: shutil.move(f"{archive_path}.zip", os.path.join(target_location, f"{FETCH_YEAR}.zip")),
    op_kwargs={'target_location': FETCH_ARCHIVE_OUTPUT_DIR,
               'archive_path': FETCH_DATA_FILE_OUTPUT_DIR.rstrip('/')},
    dag=dag_fetch,
)

# Set dependencies for the fetch DAG
fetch_page_task >> select_files_task >> fetch_files_task >> zip_files_task >> move_archive_task

# DAG for analytics
dag_analytics = DAG(
    dag_id='Analytics_Pipeline',
    default_args=default_analytics_args,
    description='Analytics pipeline',
    params={
        'archive_path': ANALYTICS_ARCHIVE_PATH,
        'required_fields': ANALYTICS_REQUIRED_FIELDS,
    },
    schedule_interval='@daily',
    catchup=False,
)

# Task 2.1: Wait for the archive
wait_for_archive_task = FileSensor(
    task_id='wait_for_archive',
    mode="poke",
    poke_interval=5,
    timeout=300,  # wait for 5 minutes
    filepath=ANALYTICS_ARCHIVE_PATH,
    dag=dag_analytics,
)

# Task 2.2: Unzip the archive
unzip_archive_task = BashOperator(
    task_id='unzip_archive',
    bash_command="unzip -o {{params.archive_path}} -d /tmp/data2",
    dag=dag_analytics,
)

# Task 2.3: Process CSV with Apache Beam
def process_csv(required_fields, **kwargs):
    # Placeholder function body
    pass  # Implement CSV processing logic with Apache Beam here

process_csv_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv,
    op_kwargs={'required_fields': "{{ params.required_fields }}"},
    dag=dag_analytics,
)

# Task 2.4: Compute monthly averages
def compute_monthly_avg(required_fields, **kwargs):
    # Placeholder function body
    pass  # Implement monthly averages computation logic here

compute_monthly_avg_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_avg,
    op_kwargs={'required_fields': "{{ params.required_fields }}"},
    dag=dag_analytics,
)

# Task 2.5: Create heatmap visualizations
def create_heatmap_visualization(required_fields, **kwargs):
    # Placeholder function body
    pass  # Implement heatmap visualization logic here

create_heatmap_task = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visualization,
    op_kwargs={'required_fields': "{{ params.required_fields }}"},
    dag=dag_analytics,
)

# Task 2.6: Delete CSV files after processing
delete_csv_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=lambda: shutil.rmtree('/tmp/data2'),
    dag=dag_analytics,
)

# Set dependencies for the analytics DAG
wait_for_archive_task >> unzip_archive_task >> [process_csv_task, compute_monthly_avg_task]
process_csv_task >> delete_csv_task
compute_monthly_avg_task >> create_heatmap_task >> delete_csv_task

