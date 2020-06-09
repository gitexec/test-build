from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.custom_operators import GetCheckpointOperator, SetCheckpointOperator
from libs.shared.test import skipif_prd


@skipif_prd
def test_get_checkpoint_default(env, bigquery_helper):
    bigquery_helper.truncate([
        ('system', [('checkpoint', [])]),
        ('lake', [('tree_users', []), ('users', [])]),
    ])
    dag_id = 'get_checkpoint_default'
    with DAG(dag_id=dag_id, start_date=datetime.now()) as dag:
        task = GetCheckpointOperator(
            env=env['env'],
            target='lake.tree_users',
            sources=['lake.tree_users', 'lake.users'],
            dag=dag, task_id='test_task')
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
        xcom = ti.xcom_pull(key='lake.tree_users', task_ids='test_task')
        assert xcom['dag_id'] == dag_id
        assert xcom['first_ingestion_timestamp'] == '1970-01-01 00:00:00'
        assert xcom['has_data'] is False


@skipif_prd
def test_get_checkpoint_existing(env, bigquery_helper, seed):
    dag_id = 'get_existing_checkpoint_existing'
    checkpoint = {'dag_id': dag_id, 'table': 'lake.pyr_contacts', 'checkpoint': '2020-03-27 06:05:00+00:00'}
    seeds = [('lake', [('pyr_contacts', [])]), ('system', [('checkpoint', [checkpoint])])]
    seed(seeds)

    task_id = 'existing_checkpoint_task'
    with DAG(dag_id=dag_id, start_date=datetime.now()) as dag:
        task = GetCheckpointOperator(
            env=env['env'],
            target=checkpoint['table'],
            sources=[checkpoint['table'], 'lake.pyr_contact_emails'],
            dag=dag,
            task_id=task_id)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
        xcom = ti.xcom_pull(key=checkpoint['table'], task_ids=task_id)
        assert xcom['dag_id'] == dag_id
        assert xcom['first_ingestion_timestamp'] == checkpoint['checkpoint']
        assert xcom['has_data'] is False


@skipif_prd
def test_get_checkpoint_prefetch_has_data_is_true_no_existing_checkpoint(env, bigquery_helper, seed):
    table = 'lake.pyr_contacts'
    seeds = [('system', [('checkpoint', [])]),
             ('lake', [('pyr_contacts', [
                 {'id': 1, 'first_name': 'John', 'last_name': 'the Beloved',
                  'leo_eid': 'z/2020/03/27/06/05/000000-0000',
                  'ingestion_timestamp': '2020-03-28 08:59:45',
                  'icentris_client': 'bluesun'}
             ])])]
    seed(seeds)

    dag_id = 'get_existing_checkpoint_has_data'
    task_id = 'prefetch_has_data_no_checkpoint_task'
    with DAG(dag_id=dag_id, start_date=datetime.now()) as dag:
        task = GetCheckpointOperator(
            env=env['env'],
            target=table,
            sources=[table, 'lake.pyr_contact_emails'],
            dag=dag, task_id=task_id)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
        xcom = ti.xcom_pull(key=table, task_ids=task_id)
        assert xcom['dag_id'] == dag_id
        assert xcom['first_ingestion_timestamp'] == '1970-01-01 00:00:00'
        assert xcom['has_data'] is True
        assert xcom['last_ingestion_timestamp'] == '2020-03-28 08:59:45+00:00'


@skipif_prd
def test_set_checkpoint_no_current_checkpoint_prefetch_has_data_true(env, bigquery_helper, seed):
    table = 'lake.tree_users'
    seeds = [('system', [('checkpoint', [])])]
    seed(seeds)

    task_id = 'set_checkpoint_no_current_record'
    with DAG(dag_id='set_checkpoint_test', start_date=datetime.now()) as dag:
        task = SetCheckpointOperator(env=env['env'], table=table, dag=dag, task_id=task_id)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.xcom_push(key=table, value={
            'first_ingestion_timestamp': '1970-01-01 00:00:00+00:00',
            'last_ingestion_timestamp': '2020-03-27 06:05:00+00:00',
            'has_data': True
        })

        task.execute(ti.get_template_context())

        rs = bigquery_helper.query(f"SELECT * FROM {env['project']}.system.checkpoint WHERE table = '{table}'")
        assert str(rs[0]['checkpoint']) == '2020-03-27 06:05:00+00:00'


@skipif_prd
def test_set_checkpoint_current_checkpoint_prefetch_has_data_true(env, bigquery_helper, seed):
    dag_id = 'set_checkpoint_test'
    table = 'lake.tree_users'
    seeds = [('system', [('checkpoint', [{'dag_id': dag_id, 'table': table, 'checkpoint': '1970-05-03 11:23:00+00:00'}])])]
    seed(seeds)

    task_id = 'set_checkpoint_no_current_record'
    with DAG(dag_id=dag_id, start_date=datetime.now()) as dag:
        task = SetCheckpointOperator(env=env['env'], table=table, dag=dag, task_id=task_id)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.xcom_push(key=table, value={
            'first_ingestion_timestamp': '1970-01-01 00:00:00+00:00',
            'last_ingestion_timestamp': '2020-05-03 11:23:00+00:00',
            'has_data': True
        })

        task.execute(ti.get_template_context())

        rs = bigquery_helper.query(f"SELECT * FROM {env['project']}.system.checkpoint WHERE table = '{table}'")
        assert str(rs[0]['checkpoint']) == '2020-05-03 11:23:00+00:00'


@skipif_prd
def test_set_checkpoint_prefetch_has_data_is_false(env, bigquery_helper, seed):
    table = 'lake.tree_users'
    seeds = [('system', [('checkpoint', [])])]
    seed(seeds)

    task_id = 'set_checkpoint_no_current_record'
    with DAG(dag_id='set_checkpoint_test', start_date=datetime.now()) as dag:
        task = SetCheckpointOperator(env=env['env'], table=table, dag=dag, task_id=task_id)
        ti = TaskInstance(task=task, execution_date=datetime.now())
        ti.xcom_push(key=table, value={
            'first_ingestion_timestamp': '1970-01-01 00:00:00+00:00',
            'has_data': False
        })

        task.execute(ti.get_template_context())

        rs = bigquery_helper.query(f"SELECT * FROM {env['project']}.system.checkpoint WHERE table = '{table}'")
        assert len(rs) == 0
