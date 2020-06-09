"""
$dag_filename$: wrench_to_lake.py
"""
import os
from libs import GCLOUD as gcloud, CloudStorage
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from airflow.operators.email_operator import EmailOperator
from libs import report_failure

airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
env = os.environ['ENV']
project_id = gcloud.project(env)
bucket_name = f'{project_id}-wrench-imports'
processed_file_dir = 'processed_files'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 3, 8),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}


def should_run():
    bucket = CloudStorage.factory(project_id).get_bucket(bucket_name)
    blobs = CloudStorage.factory(project_id).list_blobs(bucket)
    files = list(map(lambda b: b.name, blobs))
    if isinstance(files, list) and any('.csv' in file for file in files):
        return 'schedule_df_wrench_to_lake'
    else:
        return 'finish'


def move_files():
    bucket = CloudStorage.factory(project_id).get_bucket(bucket_name)
    blobs = CloudStorage.factory(project_id).list_blobs(bucket)
    for b in blobs:
        b.bucket.rename_blob(b, f'{processed_file_dir}/{b.name}')


DAG_ID = 'wrench_to_lake'


def create_dag():
    dag = DAG(DAG_ID,
              default_args=default_args,
              schedule_interval='@hourly',
              catchup=False)
    with dag:
        finish_task = DummyOperator(task_id='finish')
        pusher_task_id = f'schedule_df_wrench_to_lake'
        should_run_task = BranchPythonOperator(
            task_id='should_run',
            python_callable=should_run
        )
        schedule_df_task = ScheduleDataflowJobOperator(
            task_id=pusher_task_id,
            project=project_id,
            template_name='load_wrench_to_lake',
            job_name=f'wrench-to-lake',
            job_parameters={},
            provide_context=True
        )
        monitor_df_job_task = DataflowJobStateSensor(
            task_id=f'monitor_df_job',
            pusher_task_id=pusher_task_id,
            poke_interval=airflow_vars['dags']['wrench_to_lake']['poke_interval'],
            timeout=airflow_vars['dags']['wrench_to_lake']['poke_timeout'],
            dag=dag
        )
        move_files_task = PythonOperator(
            task_id='move_processed_files',
            python_callable=move_files
        )
        should_run_task >> schedule_df_task >> monitor_df_job_task >> move_files_task >> finish_task

    return dag


globals()[DAG_ID] = create_dag()
