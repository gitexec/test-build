from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.state import State
from unittest import mock


def test_is_dag(load_dag):
    dag_bag = load_dag('wrench_to_lake')
    dag = dag_bag.get_dag('wrench_to_lake')
    assert isinstance(dag, DAG)


def test_has_expect_task_count(load_dag):
    dag_bag = load_dag('wrench_to_lake')
    dag = dag_bag.get_dag('wrench_to_lake')
    assert len(dag.tasks) == 5


def test_should_run_skip(load_dag, airflow_session, storage):
    with mock.patch(
        'google.cloud.storage.client.Client._connection',
        new_callable=mock.PropertyMock,
    ) as client_mock:
        client_mock.return_value = storage.mock_connection({'items': []})

        start_date = timezone.datetime(2020, 1, 1)
        dag_bag = load_dag('wrench_to_lake')
        dag = dag_bag.get_dag('wrench_to_lake')
        dag.clear()

        should_run_task = dag.get_task('should_run')
        assert isinstance(should_run_task, BranchPythonOperator)

        should_run_ti = TaskInstance(should_run_task, start_date)
        should_run_ti.run()

        with airflow_session() as session:
            tis = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag.dag_id,
                TaskInstance.execution_date == start_date
            )
            for ti in tis:
                if ti.task_id == 'should_run':
                    assert ti.state == State.SUCCESS
                elif ti.task_id == 'schedule_df_wrench_to_lake':
                    assert ti.state == State.SKIPPED


def test_should_run_continue(load_dag, airflow_session, storage):
    with mock.patch(
        'google.cloud.storage.client.Client._connection',
        new_callable=mock.PropertyMock,
    ) as client_mock:
        client_mock.return_value = storage.mock_connection({'items': [{'name': 'bar.csv'}]})

        start_date = timezone.datetime(2020, 1, 1)
        dag_bag = load_dag('wrench_to_lake')
        dag = dag_bag.get_dag('wrench_to_lake')
        dag.clear()

        should_run_task = dag.get_task('should_run')
        assert isinstance(should_run_task, BranchPythonOperator)

        should_run_ti = TaskInstance(should_run_task, start_date)
        should_run_ti.run()

        with airflow_session() as session:
            tis = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag.dag_id,
                TaskInstance.execution_date == start_date
            )
            for ti in tis:
                if ti.task_id == 'should_run':
                    assert ti.state == State.SUCCESS
                elif ti.task_id == 'schedule_df_wrench_to_lake':
                    assert ti.state != State.SKIPPED
