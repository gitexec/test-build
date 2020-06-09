"""
$dag_filename$: vibe_to_lake---client--.py
"""
import logging
import os
from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from libs import GCLOUD as gcloud
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from libs import report_failure

env = os.environ['ENV']
project = gcloud.project(env)
airflow_vars = Variable.get("airflow_vars", deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 3, 8),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}

DAG_ID = 'vibe_to_lake---client--'


def _marker(st):
    logging.info('********************************{}*****************************************'.format(st))


def create_dag(client):
    dag = DAG(DAG_ID,
              default_args=default_args,
              schedule_interval=None)
    with dag:
        start_task = DummyOperator(task_id='start')
        finish_task = DummyOperator(task_id='finish')

        for table in airflow_vars['dags']['vibe_to_lake']['tables']:
            pusher_task_id = f'schedule_dataflow_{table}'
            schedule_df_task = ScheduleDataflowJobOperator(
                task_id=pusher_task_id,
                project=project,
                template_name='load_vibe_to_lake',
                job_name=f'vibe-to-lake---client---{table}',
                job_parameters={
                    'client': '--client--',
                    'table': f'`{project}.pyr_--client--_{env}.{table}`',
                    'dest': f'{project}:lake.{table}'
                },
                provide_context=True
            )
            monitor_df_job_task = DataflowJobStateSensor(
                task_id=f'monitor_df_job_{table}',
                pusher_task_id=pusher_task_id,
                poke_interval=airflow_vars['dags']['vibe_to_lake']['poke_interval'],
                timeout=airflow_vars['dags']['vibe_to_lake']['poke_timeout'],
                dag=dag
            )
            start_task.set_downstream(schedule_df_task)
            schedule_df_task.set_downstream(monitor_df_job_task)
            monitor_df_job_task.set_downstream(finish_task)

        start_task >> finish_task
    return dag


globals()[DAG_ID] = create_dag('--client--')
