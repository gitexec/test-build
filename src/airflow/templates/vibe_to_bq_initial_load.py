"""
$dag_filename$: vibe_to_bq_initial_load---client--.py
"""
import json
import logging
import os
from libs import GCLOUD as gcloud, GoogleCloudServiceFactory, CloudStorage
from airflow import DAG, settings
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from libs import report_failure

log = logging.getLogger()
airflow_vars = Variable.get("airflow_vars", deserialize_json=True)
env = os.environ['ENV']

project_id = gcloud.project(env)

cloudsql_instance = 'icentris-vibe-dbs'
bucket = f"{project_id}-vibe-schemas"
import_file_name = f'pyr_--client--_{env}.sql.gz'
database_name = f'pyr_--client--_prd'
create_db_cmd = f'gcloud sql databases create {database_name} --instance={cloudsql_instance}'
import_db_cmd = f"""gcloud sql import sql -q {cloudsql_instance} \
                    gs://{bucket}/{import_file_name} \
                    --database={database_name}"""
delete_db_cmd = f'gcloud sql databases delete -q {database_name} --instance={cloudsql_instance}'
start_sql_cmd = f'gcloud sql instances patch {cloudsql_instance} --activation-policy ALWAYS'
stop_sql_cmd = f'gcloud sql instances patch {cloudsql_instance} --activation-policy NEVER'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() + timedelta(days=-1),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}

service = GoogleCloudServiceFactory.build('dataflow')


def _marker(st):
    logging.info('********************************{}*****************************************'.format(st))


def should_run():
    exist = CloudStorage.factory(project_id).blob_exists(bucket, f'{import_file_name}')
    if exist:
        return 'start_sql_instance'
    else:
        return 'finish'


def delete_db_import_file():
    CloudStorage.factory(project_id).delete_blob(bucket, f'{import_file_name}')


DAG_ID = 'vibe_to_bq_initial_load---client--'


def create_dag(client):
    dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None)
    with dag:
        tables = []
        with open(f'{settings.DAGS_FOLDER}/table_lists/table-list.json', 'r') as f:
            file_json_content = f.read()
            tables = json.loads(file_json_content)

        should_run_task = BranchPythonOperator(
            task_id='should_run',
            python_callable=should_run
        )
        start_sql_instance_task = BashOperator(
            task_id='start_sql_instance',
            bash_command=start_sql_cmd
        )
        pre_delete_database_task = BashOperator(
            task_id=f'pre_delete_{database_name}_database',
            bash_command=delete_db_cmd
        )
        create_db_task = BashOperator(
            task_id=f'create_{database_name}_database',
            bash_command=create_db_cmd
        )
        import_db_task = BashOperator(
            task_id=f'import_{database_name}_database',
            bash_command=import_db_cmd
        )
        delete_db_import_file_task = PythonOperator(
            task_id='delete_db_import_file',
            python_callable=delete_db_import_file
        )
        post_delete_database_task = BashOperator(
            task_id=f'post_delete_{database_name}_database',
            bash_command=delete_db_cmd
        )
        stop_sql_instance_task = BashOperator(
            task_id='stop_sql_instance',
            bash_command=stop_sql_cmd
        )
        finish_task = DummyOperator(
            task_id='finish'
        )

        try:
            for t in tables:
                pusher_task_id = f'schedule_dataflow_job_for_{t["table"]}'
                schedule_df_task = ScheduleDataflowJobOperator(
                    task_id=pusher_task_id,
                    project=project_id,
                    template_name='load_sql_to_bq',
                    job_name=f'load---client---{t["table"]}-sql-to-bq',
                    job_parameters={
                        'env': env,
                        'client': '--client--',
                        'bq_table': f'{project_id}:{database_name}.{t["table"]}',
                        'table': t["table"],
                        'key_field': t["keyField"]
                    },
                    provide_context=True
                )
                monitor_df_job_task = DataflowJobStateSensor(
                    task_id=f'monitor_df_job_{t["table"]}',
                    pusher_task_id=pusher_task_id,
                    poke_interval=airflow_vars['dags']['vibe_to_bq_initial_load']['poke_interval'],
                    timeout=airflow_vars['dags']['vibe_to_bq_initial_load']['poke_timeout']
                )
                import_db_task.set_downstream(schedule_df_task)
                schedule_df_task.set_downstream(monitor_df_job_task)
                monitor_df_job_task.set_downstream(delete_db_import_file_task)
        except Exception as e:
            log.error(e)

        (
            should_run_task
            >> start_sql_instance_task
            >> pre_delete_database_task
            >> create_db_task
            >> import_db_task
            >> delete_db_import_file_task
            >> post_delete_database_task
            >> stop_sql_instance_task
            >> finish_task
        )

    return dag


globals()[DAG_ID] = create_dag('--client--')
