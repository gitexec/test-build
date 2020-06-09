from datetime import datetime, timedelta
import time
import pytest
from airflow import DAG
from airflow.models import TaskInstance, XCom, DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from airflow.utils.state import State
from airflow.utils import timezone
from pprint import pprint
from googleapiclient.http import RequestMockBuilder, HttpMock
import os
import httplib2
from unittest.mock import patch


DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = timedelta(hours=12)


@pytest.mark.skip(reason='Needs template pre-deployed')
def test_returns_job(env):
    with DAG(dag_id='schedule_dataflow_test', start_date=datetime.now(), schedule_interval=None) as dag:
        task = ScheduleDataflowJobOperator(
            project=env['project'],
            template_name='load_vibe_to_lake',
            job_name='schedule-dataflow-test-{}'.format(int(time.time())),
            job_parameters={
                'client': 'bluesun',
                'table': 'pyr_bluesun_local.tree_user_types',
                'dest': '{}:lake.tree_user_types'.format(env['project'])
            },
            dag=dag,
            task_id='test_task'
        )

    ti = TaskInstance(task=task, execution_date=datetime.now())
    job = task.execute(ti.get_template_context())
    assert job['projectId'] == env['project']


@patch('googleapiclient.http')
def test_already_running_then_skip(env, setup_teardown, airflow_session):

    def datafile(filename):
        return os.path.join('/workspace/airflow/dags/libs/shared/data', filename)

    # Save these snippets for later in case we need to mock an success. - Stu M. 4/29/19
    # http = HttpMock(datafile('dataflow.json'), {'status': '200'})
    # requestBuilder = RequestMockBuilder(
    #     {'dataflow.projects.templates.launch': (None, '{"job": ""}')}
    # )
    # with pytest.raises(HttpError) as e:
    #         job = task.execute(ti.get_template_context())
    #         assert e.resp.status == 409

    http = HttpMock(datafile('dataflow.json'), {'status': '200'})
    errorResponse = httplib2.Response({'status': '409', 'reason': 'Server Error'})
    requestBuilder = RequestMockBuilder(
        {'dataflow.projects.templates.launch': (errorResponse, b'')}
    )

    dag = DAG('shortcircuit_operator_test_with_dag_run',
              default_args={
                  'owner': 'airflow',
                  'start_date': DEFAULT_DATE
              },
              schedule_interval=INTERVAL)
    task = ScheduleDataflowJobOperator(
        project=env['project'],
        template_name='load_vibe_to_lake',
        job_name='schedule-dataflow-test-{}'.format(int(time.time())),
        job_parameters={
            'client': 'bluesun',
            'table': 'pyr_bluesun_local.tree_user_types',
            'dest': '{}:lake.tree_user_types'.format(env['project'])
        },
        dag=dag,
        task_id='schedule_dataflow_operation',
        http=http, requestBuilder=requestBuilder
    )

    middle_task = DummyOperator(
        task_id='middle_task',
        dag=dag
    )

    finish_task = DummyOperator(
        task_id='finish',
        dag=dag
    )

    task >> middle_task >> finish_task

    dag.clear()

    task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    with airflow_session() as session:
        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.execution_date == DEFAULT_DATE
        )
        for ti in tis:
            if ti.task_id == 'schedule_dataflow_operation':
                assert ti.state == State.SUCCESS
            elif ti.task_id == 'middle_task':
                assert ti.state == State.SKIPPED
            elif ti.task_id == 'finish':
                assert ti.state == State.SKIPPED


@pytest.fixture
def setup_teardown(airflow_session):
    # setup
    with airflow_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
    yield
    # teardown
    with airflow_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


@pytest.fixture
def create_dag():
    return DAG(dag_id='test_dag', start_date=datetime.now(), schedule_interval=None)


@pytest.fixture
def create_df_task(env):
    def _create_df_task(dag):
        return ScheduleDataflowJobOperator(
            project=env['project'],
            template_name='load_vibe_to_lake',
            job_name='schedule-dataflow-test-{}'.format(int(time.time())),
            job_parameters={
                'client': 'bluesun',
                'table': 'pyr_bluesun_local.tree_user_types',
                'dest': '{}:lake.tree_user_types'.format(env['project'])
            },
            pull_parameters=[
                {'key': 'one'},
                {'key': 'two', 'param_name': 'two-specific-key'},
                {'task_id': 'three', 'param_name': 'three'}
            ],
            dag=dag,
            task_id='test_task'
        )

    return _create_df_task


@pytest.fixture
def push_xcom_key():
    def _push_xcom_key(key, value, **kwargs):
        kwargs['ti'].xcom_push(key=key, value=value)

    return _push_xcom_key


@pytest.fixture
def push_xcom():
    def _push_xcom(value, **kwargs):
        kwargs['ti'].xcom_push(key=None, value=value)
        # XCom.set()

    return _push_xcom


def test_pull_parameters_by_key(env, create_dag, create_df_task, push_xcom_key):
    dag = create_dag
    df_task = create_df_task(dag)
    push_task = PythonOperator(
        task_id='one',
        python_callable=push_xcom_key,
        op_args=['one', 'one_value'],
        dag=dag,
        provide_context=True
    )

    push_ti = TaskInstance(task=push_task, execution_date=datetime.now())
    push_task.execute(push_ti.get_template_context())
    xcom = XCom.get_many(execution_date=push_ti.execution_date, dag_ids=[dag.dag_id])
    assert len(xcom) == 1
    assert xcom[0].value == 'one_value'
    merged = df_task.merge_parameters(push_ti)
    assert merged['one'] == 'one_value'


def test_pull_parameters_by_specific_key(env, create_dag, create_df_task, push_xcom_key):
    dag = create_dag
    df_task = create_df_task(dag)
    push_task = PythonOperator(
        task_id='two',
        python_callable=push_xcom_key,
        op_args=['two', 'two_value'],
        dag=dag,
        provide_context=True
    )

    push_ti = TaskInstance(task=push_task, execution_date=datetime.now())
    push_task.execute(push_ti.get_template_context())
    xcom = XCom.get_many(execution_date=push_ti.execution_date, dag_ids=[dag.dag_id])
    assert len(xcom) == 1
    assert xcom[0].value == 'two_value'
    merged = df_task.merge_parameters(push_ti)
    assert merged['two-specific-key'] == 'two_value'


@pytest.mark.skip(reason='I still need to figure out push/pulling by task so it replicates the real task')
def test_pull_parameters_by_task(env, create_dag, create_df_task, push_xcom):
    dag = create_dag
    # df_task = create_df_task(dag)
    push_task = PythonOperator(
        task_id='three',
        python_callable=push_xcom,
        op_args=['three_value'],
        dag=dag,
        provide_context=True
    )

    push_ti = TaskInstance(task=push_task, execution_date=datetime.now())
    push_task.execute(push_ti.get_template_context())
    xcom = XCom.get_many(execution_date=push_ti.execution_date, dag_ids=[dag.dag_id], task_ids=[push_task.task_id])
    pprint(xcom[0])
    assert len(xcom) == 1
    assert xcom[0].value == 'three_value'
    assert False
    # merged = df_task.merge_parameters(push_ti)
    # print(merged)
    # assert merged['three'] == 'three_value'

    # merge_task = DummyOperator(
    #     task_id='merge',
    #     dag=dag,
    #     provide_context=True
    # )

    # merge_ti = TaskInstance(task=merge_task, execution_date=datetime.now())
    # merge_task.execute(merge_ti.get_template_context())
    # merged = task.merge_parameters(one_ti)
    # print('x' * 100)
    # print(merged)
    # assert False
