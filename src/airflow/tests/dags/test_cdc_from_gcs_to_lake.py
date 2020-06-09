import os
import pytest
import cdc_from_gcs_to_lake
from airflow.models.taskinstance import TaskInstance
from datetime import datetime
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from libs import GCLOUD as gcloud, CloudStorage
from airflow.models import Variable
from unittest.mock import patch
from unittest import mock
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.client import Client


airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
project_id = gcloud.project(os.environ['ENV'])
bucket = f'{project_id}-cdc-imports'
processed_bucket = f'{project_id}-cdc-imports-processed'
FILE_NAME = 'vibe-commission-bonuses-final-8b8e336ea2366596964f1ed2c67c3039bc5cfe57e823db8e647835f1fee26040-1587028509211'
FILE_SEED = """
14d1bbba50e40234839420171eb87431:0c81720eff1215c298621670f689ac76a3300ce0320c3a3c1c381d5f356f9fa405d14a9deabd0757207776d12a76bc076e2d0baaa6a79a0cb66b0ec2ee78005f05722934b501e1cb083bfedcc319e41dc0a207e899fcb9558f6c8826e3cee6beb67a0d1a878e4a5e86bb7f0579c28bcde88539add19e7aea69c495a413d2dc37892162d68b75e6003db81846bb96bfb946ef3d387a2b116b92a5b609b4c4e3c8570139f804daa04b105feeac06845efda0dce5360809de73d4c7831c9e84c4974313ebe7ea807093e2f214379f4c5e8c805fa4004cfc2f1c8cbf23ad68145a3a
"""


def test_has_exected_task_count(load_dag):
    dag_bag = load_dag('cdc_from_gcs_to_lake')
    dag = dag_bag.get_dag('cdc_from_gcs_to_lake')
    assert len(dag.tasks) == 126


def test_continue_if_file_task(load_dag):
    task_id = 'continue_if_file_vibe-messages-final'
    dag_bag = load_dag('cdc_from_gcs_to_lake')
    dag = dag_bag.get_dag('cdc_from_gcs_to_lake')
    task = dag.get_task(task_id)
    assert isinstance(task, PythonOperator)
    # When creating a Task, order is important here
    # op_args is postional. - Stu M. 6/4/20
    assert task.op_args[0] == 'vibe-messages-final'  # prefix
    assert isinstance(task.op_args[1], Bucket)  # bucket
    assert task.op_args[2] == 'pyr_messages'  # table
    assert task.python_callable.__name__ == cdc_from_gcs_to_lake.should_continue.__name__


def test_should_continue():
    with patch.object(CloudStorage, 'has_file', return_value=True):
        bucket = mock.create_autospec(Bucket)
        bucket.name = 'TO_BUCKET'
        table = 'table'
        assert cdc_from_gcs_to_lake.should_continue(
            bucket=bucket,
            table=table
        ) == f'schedule_df_gcs_to_lake_table'
    with patch.object(CloudStorage, 'has_file', return_value=False):
        assert cdc_from_gcs_to_lake.should_continue(
            bucket=bucket,
            table=table
        ) == 'finish'


def test_move_files_task(load_dag):
    task_id = 'move_processed_files_vibe-messages-final'
    dag_bag = load_dag('cdc_from_gcs_to_lake')
    dag = dag_bag.get_dag('cdc_from_gcs_to_lake')
    task = dag.get_task(task_id)
    assert isinstance(task, PythonOperator)
    # When creating a Task,
    # order is important here, op_args is postional. - Stu M. 6/4/20
    assert task.op_args[0] == 'vibe-messages-final'  # prefix
    assert isinstance(task.op_args[1], Bucket)  # from_bucket
    assert isinstance(task.op_args[2], Bucket)  # to_bucket
    assert task.python_callable.__name__ == CloudStorage.move_files.__name__


def test_environment():
    """
    Test bucket naming is important because we need to setup in environment.py
    """
    assert cdc_from_gcs_to_lake.bucket == f'{project_id}-cdc-imports'
    assert cdc_from_gcs_to_lake.processed_bucket == f'{project_id}-cdc-imports-processed'
