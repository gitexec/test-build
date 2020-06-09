"""
$dag_filename$: bq_to_wrench.py
"""
import os
import uuid
from libs import tempdir
from libs import Config
from libs import GCLOUD as gcloud, CloudStorage
from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import GetCheckpointOperator, SetCheckpointOperator
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
import logging
import boto3
from libs import BigQuery
from libs import report_failure

DAG_ID = 'bq_to_wrench'
env = os.environ['ENV']
project = gcloud.project(env)
gcs_bucket = f'{project}-wrench-exports'
log = logging.getLogger()

log.info(f'gcs_bucket = {gcs_bucket}')


default_args = {
    'owner': 'airflow',
    'description': 'Offload data from BigQuery to CloudStorage; Move CloudStorage files to Wrench S3 bucket.',
    'start_date': datetime(2020, 3, 27),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}


def mv_to_s3(gcp_bucket, table, aws_access_key_id, aws_secret_access_key, aws_s3_bucket):
    def _is_cloud_storage_dir(object_name):
        return object_name.endswith('/')

    aws_s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # This directory should be removed once the operation is complete, because of GDPR. Stu M. 11/29/19
    with tempdir() as tmp:
        bucket = CloudStorage.factory(project).get_bucket(gcp_bucket)
        blobs = bucket.list_blobs(prefix=table)
        bucket_dirs_marked_for_deletion = []
        for blob in blobs:
            key = blob.name
            file_or_dir = '{}/{}'.format(tmp, key)
            if _is_cloud_storage_dir(key):
                os.mkdir(file_or_dir)
                bucket_dirs_marked_for_deletion.append(key)
            else:
                splits = key.split('/')
                splits[len(splits)-1] = str(uuid.uuid4()) + '-' + splits[len(splits)-1]
                key = '/'.join(splits)
                dirname = os.path.dirname(file_or_dir)
                if not os.path.isdir(dirname):
                    os.mkdir(dirname)
                blob.download_to_filename(file_or_dir)
                aws_s3_client.upload_file(file_or_dir, aws_s3_bucket, key)
                blob.delete()

        # Cleanup here because, folders in gcs are not deleted. Stu. M. 2/29/20
        for key in bucket_dirs_marked_for_deletion:
            blob = bucket.blob(key)
            blob.delete()


def gcs_to_wrench_s3(env, table):
    ml_config = Config.get_config(env)
    aws_access_key_id = ml_config['cloudstorage']['wrench']['aws_access_key_id']
    aws_secret_access_key = ml_config['cloudstorage']['wrench']['aws_secret_access_key']
    aws_s3_bucket = ml_config['cloudstorage']['wrench']['destination_s3_bucket']
    mv_to_s3(
        gcs_bucket,
        table,
        aws_access_key_id,
        aws_secret_access_key,
        aws_s3_bucket)


def continue_if_data(table, **kwargs):
    checkpoint = kwargs['ti'].xcom_pull(key=table)
    if isinstance(checkpoint, dict) and checkpoint['has_data'] is True:
        return 'clear_gcs_bucket_{}'.format(table)
    else:
        return 'finish'


def clear_gcs_bucket_by_table(env, table):
    bucket = CloudStorage.factory(project).get_bucket(gcs_bucket)
    blobs = bucket.list_blobs(prefix=table)
    for blob in blobs:
        blob.delete()


def build_query(checkpoint=None, tree_user_ids=None, project=None, table=None, created_after_certain_date=None):

    wheres = []
    if checkpoint is not None:
        # Break up query string so it passes linting. - Stu M 5/21/20
        f = checkpoint['first_ingestion_timestamp']
        last = checkpoint['last_ingestion_timestamp']
        wheres.append(
            f"ingestion_timestamp BETWEEN '{f}' AND '{last}'"
        )

    if tree_user_ids is not None:
        wheres.append(f'tree_user_id in ({",".join(str(x) for x in tree_user_ids)})')

    if created_after_certain_date is not None:
        tables = get_airflow_vars()['dags'][DAG_ID]['tables']
        created_field = list(filter(lambda x: x['name'] == table, tables))[0]['created_field']
        if created_field == 'created':
            wheres.append(f"created >= '{created_after_certain_date}'")
        elif created_field == 'created_at':
            wheres.append(f"created_at >= '{created_after_certain_date}'")
        else:
            raise Exception(f'created_field {created_field} not found in variables')

    if len(wheres) > 0:
        return BigQuery.querybuilder(
            select='*',
            where=wheres,
            from_=f'`{project}.{table}`'
        )
    else:
        return BigQuery.querybuilder(
            select='*',
            from_=f'`{project}.{table}`'
        )


def get_airflow_vars():
    airflow_vars = Variable.get("airflow_vars", deserialize_json=True)

    # Elise needs to provide us with these values later.  Until then, our dag should work otherwise. - Stu M 5/19/20
    if not hasattr(airflow_vars['dags'][DAG_ID], 'tree_user_ids'):
        airflow_vars['dags'][DAG_ID]['tree_user_ids'] = None

    if not hasattr(airflow_vars['dags'][DAG_ID], 'created_after_certain_date'):
        airflow_vars['dags'][DAG_ID]['created_after_certain_date'] = None

    return airflow_vars


def parse_query(table, **kwargs):
    checkpoint = kwargs['ti'].xcom_pull(key=table)
    tree_user_ids = get_airflow_vars()['dags'][DAG_ID]['tree_user_ids']
    created_after_certain_date = get_airflow_vars()['dags'][DAG_ID]['created_after_certain_date']
    return build_query(
        checkpoint=checkpoint,
        project=project,
        table=table,
        tree_user_ids=tree_user_ids,
        created_after_certain_date=created_after_certain_date)


def create_dag():
    dag = DAG(DAG_ID, catchup=False, default_args=default_args, schedule_interval='@hourly')
    with dag:
        start_task = DummyOperator(
            task_id='start'
        )

        finish_task = DummyOperator(
            task_id='finish'
        )

        for table in get_airflow_vars()['dags'][DAG_ID]['tables']:
            table = table['name']
            parsed_table = gcloud.parse_table_name(table)

            get_checkpoint_task = GetCheckpointOperator(
                task_id='get_checkpoint_{}'.format(table),
                env=env,
                target=table,
                sources=[table]
            )

            continue_if_data_task = BranchPythonOperator(
                task_id='continue_if_data_{}'.format(table),
                python_callable=continue_if_data,
                op_args=[table],
                trigger_rule='all_done',
                provide_context=True
            )

            clear_gcs_bucket_by_table_task = PythonOperator(
                task_id='clear_gcs_bucket_{}'.format(table),
                python_callable=clear_gcs_bucket_by_table,
                op_args=[env, table]
            )

            parse_query_task = PythonOperator(
                task_id=f'parse_query_{table}',
                python_callable=parse_query,
                op_args=[table],
                provide_context=True
            )

            dataflow_task = ScheduleDataflowJobOperator(
                task_id=f'schedule_dataflow_{table}',
                project=gcloud.project(env),
                template_name='offload_bq_to_cs',
                job_name=f'bq-to-wrench-{parsed_table}',
                job_parameters={
                    'destination': 'gs://{}/{}/{}'.format(gcs_bucket, table, f'bq-to-wrench-{parsed_table}')
                },
                pull_parameters=[{
                    'param_name': 'query',
                    'task_id': f'parse_query_{table}'
                }],
                provide_context=True
            )

            monitor_dataflow_task = DataflowJobStateSensor(
                task_id=f'monitor_dataflow_{table}',
                pusher_task_id=f'schedule_dataflow_{table}',
                poke_interval=get_airflow_vars()['dags'][DAG_ID]['poke_interval'],
                timeout=get_airflow_vars()['dags'][DAG_ID]['poke_timeout'],
                dag=dag
            )

            gcs_to_wrench_s3_task = PythonOperator(
                task_id='gcs_to_wrench_s3_{}'.format(table),
                python_callable=gcs_to_wrench_s3,
                op_args=[env, table]
            )

            commit_checkpoint_task = SetCheckpointOperator(
                task_id='commit_checkpoint_{}'.format(table),
                env=env,
                table=table
            )

            (
                start_task
                >> get_checkpoint_task
                >> continue_if_data_task
                >> clear_gcs_bucket_by_table_task
                >> parse_query_task
                >> dataflow_task
                >> monitor_dataflow_task
                >> gcs_to_wrench_s3_task
                >> commit_checkpoint_task
                >> finish_task
            )
    return dag


globals()[DAG_ID] = create_dag()
