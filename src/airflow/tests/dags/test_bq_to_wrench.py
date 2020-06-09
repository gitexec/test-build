from datetime import datetime
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.custom_operators import GetCheckpointOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCom
from libs import GCLOUD as gcloud
from airflow.models import Variable
from unittest.mock import patch
import bq_to_wrench

airflow_vars = {
    'dags': {
        'bq_to_wrench': {
            'tree_user_ids': [1, 2, 3, 4, 5],
            'created_after_certain_date': '1970-01-01',
            'tables': [
                {'name': 'staging.contacts', 'created_field': 'created_at'},
                # {'name': 'pii.users', 'created_field': 'created_at'},
                # {'name': 'staging.emails', 'created_field': 'created'},
                # {'name': 'staging.flat_site_visitors', 'created_field': 'created_at'},
                {'name': 'staging.orders', 'created_field': 'created'},
                # {'name': 'staging.product_reviews', 'created_field': 'created_at'},
                # {'name': 'staging.sites', 'created_field': 'created_at'},
                # {'name': 'staging.tasks', 'created_field': 'created'},
                {'name': 'staging.users', 'created_field': 'created_at'},
            ],
        }
    }}

seeds = [
    ('staging', [
        ('contacts', [
            {'id': 1, 'tree_user_id': 1, 'first_name': 'Jon', 'last_name': 'Doe',
             'icentris_client': 'monat', 'client_partition_id': 2,
             'client_wrench_id': 'e08dc822-97b1-46f4-9154-25821286231f',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created_at': '1970-01-01'},
            {'id': 2, 'tree_user_id': 2, 'first_name': 'Wington', 'last_name': 'Doe',
             'icentris_client': 'monat', 'client_partition_id': 2,
             'client_wrench_id': 'e08dc822-97b1-46f4-9154-25821286231f',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created_at': '1970-01-01'},
            {'id': 3, 'tree_user_id': 3, 'first_name': 'Elise', 'last_name': 'Doe',
             'icentris_client': 'monat', 'client_partition_id': 2,
             'client_wrench_id': 'e08dc822-97b1-46f4-9154-25821286231f',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created_at': '1970-01-01'},
            {'id': 4, 'tree_user_id': 4, 'first_name': 'Stu', 'last_name': 'Doe',
             'icentris_client': 'monat', 'client_partition_id': 2,
             'client_wrench_id': 'e08dc822-97b1-46f4-9154-25821286231f',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created_at': '1970-01-01'},
            {'id': 5, 'tree_user_id': 5, 'first_name': 'Patrick', 'last_name': 'Doe',
             'icentris_client': 'monat', 'client_partition_id': 2,
             'client_wrench_id': 'e08dc822-97b1-46f4-9154-25821286231f',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created_at': '1970-01-01'},
        ])
    ])
]


def test_airflow_vars(env):
    ret = {'dags': {'bq_to_wrench': {}}}
    with patch.object(Variable, 'get', return_value=ret):
        result = bq_to_wrench.get_airflow_vars()
        assert result == ret


def test_query_ingestion_timestamp():
    checkpoint = {}
    checkpoint['first_ingestion_timestamp'] = '1'
    checkpoint['last_ingestion_timestamp'] = '2'
    query = bq_to_wrench.build_query(checkpoint=checkpoint)

    wheres = query._builder['where']
    expected = "ingestion_timestamp BETWEEN '1' AND '2'"
    has_value = False
    for where in wheres:
        if where == expected:
            has_value = True
    assert has_value


def test_query_limit_by_tree_user_ids():
    tree_user_ids = [1, 2, 3, 4, 5]
    query = bq_to_wrench.build_query(tree_user_ids=tree_user_ids)
    wheres = query._builder['where']
    expected = 'tree_user_id in (1,2,3,4,5)'
    has_value = False
    for where in wheres:
        if where == expected:
            has_value = True
    assert has_value


def test_query_created_after_certain_date():
    """
        This test is for newer data to be received by Wrench.
        Right now we are only sending these three tables to them.
        Also, we have a slight variation in table naming that we
        have to account for.
    """
    created_after_certain_date = '2016-05-28T01:37:02'

    def assert_has_value(wheres, expected):
        has_value = False
        for where in wheres:
            if where == expected:
                has_value = True
        assert has_value

    with patch.object(bq_to_wrench, 'get_airflow_vars', return_value=airflow_vars):
        for table in bq_to_wrench.get_airflow_vars()['dags']['bq_to_wrench']['tables']:
            table_name = table['name']
            query = bq_to_wrench.build_query(table=table_name, created_after_certain_date=created_after_certain_date)
            wheres = query._builder['where']

            if table['created_field'] == 'created':
                expected = "created >= '2016-05-28T01:37:02'"
                assert_has_value(wheres, expected)
            elif table['created_field'] == 'created_at':
                expected = "created_at >= '2016-05-28T01:37:02'"
                assert_has_value(wheres, expected)


def test_query_from_table(env):
    project = gcloud.project(env['env'])
    table = 'staging.contacts'
    query = bq_to_wrench.build_query(table=table, project=project)
    assert query._builder['from'] == f'`{project}.{table}`'


def test_parse_query(seed, env, xcomm_mock, bigquery_helper):
    seed(seeds)

    with patch.object(bq_to_wrench, 'get_airflow_vars', return_value=airflow_vars):
        checkpoint = {
            'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
            'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'
        }
        query = bq_to_wrench.parse_query('staging.contacts', **xcomm_mock(checkpoint))
        resp = bigquery_helper.query(sql=query)
        assert len(resp) == 5


def test_has_expected_task_count(load_dag, env):
    dag_bag = load_dag('bq_to_wrench')
    dag = dag_bag.get_dag('bq_to_wrench')
    assert len(dag.tasks) == 74


def test_sets_initial_checkpoint(load_dag, env, bigquery_helper):
    # Remove all checkpoints for table
    table = 'staging.users'
    bigquery_helper.query(f"DELETE FROM `{env['project']}.system.checkpoint` WHERE table = '{table}'")

    # Execute get checkpoint task. I expect it to create an initial checkpoint.
    dag_bag = load_dag('bq_to_wrench')
    dag = dag_bag.get_dag('bq_to_wrench')
    task = dag.get_task(f'get_checkpoint_{table}')
    assert isinstance(task, GetCheckpointOperator)
    ti = TaskInstance(task=task, execution_date=datetime.now())
    task.execute(ti.get_template_context())
    # val = XCom.get_many(execution_date=datetime.utcnow(), task_ids=[f'get_checkpoint_{table}'], key=table)
    # print(val)


def test_should_continue_with_cp(load_dag):
    dag_bag = load_dag('bq_to_wrench')
    dag = dag_bag.get_dag('bq_to_wrench')
    table = 'staging.users'
    task = dag.get_task(f'continue_if_data_{table}')
    assert isinstance(task, BranchPythonOperator)
    ti = TaskInstance(task=task, execution_date=datetime.now())
    XCom.set(
        key=table,
        value={'has_data': True},
        task_id=task.task_id,
        dag_id=dag.dag_id,
        execution_date=ti.execution_date)

    task.execute(ti.get_template_context())
