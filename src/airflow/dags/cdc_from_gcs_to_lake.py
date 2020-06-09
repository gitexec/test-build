"""
$dag_filename$: cdc_from_gcs_to_lake.py
"""
import os
import logging
from libs import GCLOUD as gcloud, CloudStorage
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from libs import report_failure

log = logging.getLogger()
airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
env = os.environ['ENV']
project_id = gcloud.project(env)
bucket = f'{project_id}-cdc-imports'
processed_bucket = f'{project_id}-cdc-imports-processed'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}


def list_blobs(bucket, files_startswith):
    return CloudStorage.factory(project_id).list_blobs(bucket, files_startswith)


def should_continue(prefix=None, bucket=None, table=None):
    if CloudStorage.factory(project_id).has_file(bucket=bucket, prefix=prefix):
        return f'schedule_df_gcs_to_lake_{table}'
    else:
        return 'finish'


DAG_ID = 'cdc_from_gcs_to_lake'


# Maps https://github.com/iCentris/leo-bus/blob/master/src/bots/offload/s3/package.json wbrito 04/13/2020
table_map = {
    'vibe-messages-final': 'pyr_messages',
    'vibe-message-recipients-final': 'pyr_message_recipients',
    'vibe-orders-final': 'tree_orders',
    'vibe-order-items-final': 'tree_order_items',
    'vibe-sites-final': 'pyr_sites',
    'vibe-site-visitors-final': 'pyr_site_visitors',
    'vibe-site-analytics-final': 'pyr_sites_analytics_tracking_codes',
    'vibe-tasks-final': 'pyr_user_tasks',
    'vibe-users-final': 'users',
    'vibe-contacts-final': 'pyr_contacts',
    'vibe-ecards-final': 'pyr_ecards',
    'vibe-events-final': 'pyr_events',
    'vibe-microsites-final': 'pyr_microsites',
    'vibe-resource-assets-final': 'pyr_resource_assets',
    'vibe-resources-final': 'pyr_resources',
    'vibe-sms-final': 'pyr_sms',
    'vibe-tree-users-final': 'tree_users',
    'vibe-tree-user-types-final': 'tree_user_types',
    'vibe-tree-order-types-final': 'tree_order_types',
    'vibe-tree-order-statuses-final': 'tree_order_statuses',
    'vibe-commission-bonuses-final': 'tree_commission_bonuses',
    'vibe-tree-commission-details-final': 'tree_commission_details',
    'vibe-tree-commission-runs-final': 'tree_commission_runs',
    'vibe-tree-commissions-final': 'tree_commissions',
    'vibe-contact-emails-final': 'pyr_contact_emails',
    'vibe-contact-phone-numbers-final': 'pyr_contact_phone_numbers',
    'vibe-contacts-contact-categories-final': 'pyr_contacts_contact_categories',
    'vibe-contact-categories-final': 'pyr_contact_categories',
    'vibe-tree-user-statuses-final': 'tree_user_statuses',
    'vibe-rank-definitions-final': 'pyr_rank_definitions',
    'vibe-login-histories-final': 'pyr_login_histories'
}


def create_dag():
    dag = DAG(DAG_ID,
              default_args=default_args,
              # Be sure to stagger the dags so they don't run all at once,
              # possibly causing max memory usage and pod failure. - Stu M.
              schedule_interval='0 * * * *',
              catchup=False)
    with dag:
        start_task = DummyOperator(task_id='start')
        finish_task = DummyOperator(task_id='finish')
        storage = CloudStorage.factory(project_id)
        cdc_imports_bucket = storage.get_bucket(bucket)
        cdc_imports_processed_bucket = storage.get_bucket(processed_bucket)

        for files_startwith, table in table_map.items():
            pusher_task_id = f'schedule_df_gcs_to_lake_{table}'
            continue_if_file_task = BranchPythonOperator(
                task_id=f'continue_if_file_{files_startwith}',
                python_callable=should_continue,
                op_args=[files_startwith, cdc_imports_bucket, table]
            )
            schedule_df_task = ScheduleDataflowJobOperator(
                task_id=pusher_task_id,
                project=project_id,
                template_name=f'load_cdc_from_gcs_to_lake',
                job_name=f'gcs-to-lake-{table}',
                job_parameters={
                    'files_startwith': files_startwith,
                    'dest': f'{project_id}:lake.{table}'
                },
                provide_context=True
            )
            monitor_df_job_task = DataflowJobStateSensor(
                task_id=f'monitor_df_job_{table}',
                pusher_task_id=pusher_task_id,
                poke_interval=airflow_vars['dags']['cdc_from_gcs_to_lake']['poke_interval'],
                timeout=airflow_vars['dags']['cdc_from_gcs_to_lake']['poke_timeout'],
                dag=dag
            )
            move_files_task = PythonOperator(
                task_id=f'move_processed_files_{files_startwith}',
                python_callable=storage.move_files,
                op_args=[files_startwith, cdc_imports_bucket, cdc_imports_processed_bucket],
            )
            (
                start_task
                >> continue_if_file_task
                >> schedule_df_task
                >> monitor_df_job_task
                >> move_files_task
                >> finish_task
            )
    return dag


globals()[DAG_ID] = create_dag()
