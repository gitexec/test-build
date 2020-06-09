import pytest
from airflow.models import Variable
from airflow.utils import timezone
from airflow.utils.state import State
from libs.shared.test import skipif_prd

seeds = [
    ('system', [('checkpoint', [])]),
    ('staging', [('contacts', [])]),
    ('lake', [
        ('pyr_contacts', [
            {'id': 1, 'tree_user_id': 1, 'first_name': 'Jon', 'last_name': 'Doe',
             'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'tree_user_id': 2, 'first_name': 'Jane', 'last_name': 'Doe', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 1, 'tree_user_id': 1, 'first_name': 'Matthew', 'last_name': 'Gospel',
             'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'tree_user_id': 2, 'first_name': 'Titus', 'last_name': 'Epistle',
             'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'tree_user_id': 2, 'first_name': 'TitusX', 'last_name': 'Epistle',
             'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/02/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-02-01 00:00:00.0 UTC'},
            {'id': 1, 'tree_user_id': 1, 'first_name': 'Jon-Replay', 'last_name': 'Doe', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
             'ingestion_timestamp': '2019-03-01 00:00:01.0 UTC'},
        ]),
        ('tree_users', [
            {'id': 1, 'first_name': 'Jon', 'last_name': 'Doe', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'first_name': 'Jane', 'last_name': 'Doe', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 1, 'first_name': 'Matthew', 'last_name': 'Gospel', 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'first_name': 'Titus', 'last_name': 'Epistle', 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 1, 'first_name': 'Jonathan', 'last_name': 'Doe', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/03/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-03-01 00:00:00.0 UTC'}
        ]),
        ('pyr_contact_categories', [
            {'id': 1, 'category_name': 'Monat One', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'category_name': 'Monat Two', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 1, 'category_name': 'WV One', 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 1, 'category_name': 'Monat One-2020', 'icentris_client': 'monat',
             'leo_eid': 'z/2020/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2020-01-01 00:00:00.0 UTC'}
        ]),
        ('pyr_contacts_contact_categories', [
            {'id': 1, 'contact_id': 1, 'contact_category_id': 1, 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'contact_id': 2, 'contact_category_id': 1, 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 3, 'contact_id': 1, 'contact_category_id': 1, 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 4, 'contact_id': 2, 'contact_category_id': 1, 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 5, 'contact_id': 1, 'contact_category_id': 2, 'icentris_client': 'monat',
             'leo_eid': 'z/2019/04/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-04-01 00:00:00.0 UTC'}
        ]),
        ('pyr_contact_emails', [
            {'id': 1, 'contact_id': 1, 'email': 'jon.doe@test.com', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'contact_id': 2, 'email': 'jane.doe@test.com', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 3, 'contact_id': 1, 'email': 'matthew.gospel@test.com', 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 4, 'contact_id': 2, 'email': 'titus.epistle@test.com', 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 5, 'contact_id': 1, 'email': 'jonathan.doe@test.com', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/05/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-05-01 00:00:01.0 UTC'}
        ]),
        ('pyr_contact_phone_numbers', [
            {'id': 1, 'contact_id': 1, 'phone_number': '111-111-1111', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 2, 'contact_id': 2, 'phone_number': '222-222-2222', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 3, 'contact_id': 1, 'phone_number': '1-111-111-1111', 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 4, 'contact_id': 2, 'phone_number': '2-222-222-2222', 'icentris_client': 'worldventures',
             'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
            {'id': 5, 'contact_id': 1, 'phone_number': '333-333-3333', 'icentris_client': 'monat',
             'leo_eid': 'z/2019/06/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-06-01 00:00:00.0 UTC'}
        ])
    ])
]


@skipif_prd
def test_dag_has_n_tasks(seed, load_dag):
    seed(seeds)
    dag_bag = load_dag('lake_to_staging')
    dag = dag_bag.get_dag('lake_to_staging')
    assert len(dag.tasks) == 26


@pytest.mark.skip(reason='this is not ready for prime time')
@skipif_prd
def test_dag_run(seed, load_dag, bigquery_helper):
    orig_cfg = Variable.get("airflow_vars", deserialize_json=True)
    print(orig_cfg)
    test_cfg = orig_cfg.copy()
    test_cfg['dags']['lake_to_staging']['tables'] = ['lake.contacts']
    try:
        seed(seeds)
        dag_bag = load_dag('lake_to_staging')
        dag = dag_bag.get_dag('lake_to_staging')
        now = timezone.utcnow()
        run_id = f'test_lake_to_staging_run_{now}'
        dag_run = dag.create_dagrun(
            run_id=run_id,
            execution_date=now,
            start_date=now,
            state=State.RUNNING,
            external_trigger=False,
        )

        while dag_run.state == State.RUNNING:
            dag_run.update_state()

        rs = bigquery_helper.query('SELECT * FROM staging.contacts')

        print(rs)
    finally:
        Variable.set('airflow_vars', orig_cfg)
        if dag_run.state == State.RUNNING:
            dag_run.state = State.FAILED
            dag_run.update_state()
            print(dag_run.state)
