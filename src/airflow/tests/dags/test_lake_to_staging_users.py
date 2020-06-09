from datetime import datetime
from airflow import settings
from importlib import util
from libs.shared.test import skipif_prd

# Dynamically load DAG as a module so we can access the parse_template function - jc 3/24/2020
spec = util.spec_from_file_location('lake_to_staging', f'{settings.DAGS_FOLDER}/lake_to_staging.py')
lake_to_staging = util.module_from_spec(spec)
spec.loader.exec_module(lake_to_staging)
ingestion_timestamp = str(datetime.utcnow())

seeds = [
    ('lake', [
        ('tree_users', [
            {'id': 1, 'icentris_client': 'worldventures',
             'first_name': 'bob', 'last_name': 'saget',
             'user_type_id': 1,
             'user_status_id': 1,
             'rank_id': 1, 'paid_rank_id': 2,
             'created_date': '2020-03-23', 'sponsor_id': 1,
             'parent_id': 1, 'zip': '11111',
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'},
            {'id': 1, 'icentris_client': 'worldventures',
             'first_name': 'robert', 'last_name': 'saget',
             'user_type_id': 1,
             'user_status_id': 1,
             'rank_id': 1, 'paid_rank_id': 2,
             'created_date': '2020-03-23', 'sponsor_id': 1,
             'parent_id': 1, 'zip': '22222',
             'leo_eid': 'z/1970/01/02/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-02 00:00:00.001 UTC'},
            {'id': 1, 'icentris_client': 'bluesun',
             'first_name': 'stub', 'last_name': 'record',
             'user_type_id': None,
             'user_status_id': None,
             'rank_id': None, 'paid_rank_id': None,
             'created_date': '2020-03-23', 'sponsor_id': None,
             'parent_id': None, 'zip': None,
             'leo_eid': 'z/1970/01/02/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-02 00:00:00.001 UTC'},
            {'id': 2, 'icentris_client': 'bluesun',
             'first_name': 'rankless', 'last_name': 'wonder',
             'user_type_id': 1,
             'user_status_id': 1,
             'rank_id': None, 'paid_rank_id': None,
             'created_date': '2020-03-23', 'sponsor_id': 1,
             'parent_id': 1, 'zip': '54321',
             'leo_eid': 'z/1970/01/02/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-02 00:00:00.001 UTC'}]),
        ('users', [
            {'id': 1, 'icentris_client': 'worldventures',
             'tree_user_id': 1,
             'encrypted_password': 'encrypted_password', 'failed_attempts': 0,
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'},
            {'id': 1, 'icentris_client': 'bluesun',
             'tree_user_id': 1,
             'encrypted_password': 'encrypted_password', 'failed_attempts': 0,
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'}]),
        ('tree_user_types', [
            {'id': 1, 'icentris_client': 'worldventures',
             'description': 'Distributor',
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'},
            {'id': 1, 'icentris_client': 'bluesun',
             'description': 'Distributor',
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'}]),
        ('pyr_rank_definitions', [
            {'id': 1,
             'icentris_client': 'worldventures', 'name': 'Platnum',
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'},
            {'id': 2,
             'icentris_client': 'worldventures', 'name': 'Rhodium',
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'}]),
        ('tree_user_statuses', [
            {'id': 1,
             'icentris_client': 'worldventures',
             'description': 'Active',
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'},
            {'id': 1,
             'icentris_client': 'bluesun',
             'description': 'Active',
             'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
             'ingestion_timestamp': '1970-01-01 00:00:00.001 UTC'}])
    ])]


@skipif_prd
def test_staging_users_query(seed, bigquery_helper):
    seed(seeds)
    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00 UTC',
        'last_ingestion_timestamp': '2020-03-24 12:53:57.123 UTC',
        "first_eid": 'z/1970/01/01/00/00/0000000000000-0000001',
        "last_eid": 'z/2020/03/24/12/53/1585054437123-0000001'
    }

    sql = lake_to_staging.parse_template(
        f'{settings.DAGS_FOLDER}/templates/sql/lake_to_staging.users.sql', **checkpoint)
    print(sql)

    rs = bigquery_helper.query(sql)
    assert len(rs) == 1
    assert rs[0]['zip'] == '22222'


@skipif_prd
def test_pii_users_query(seed, bigquery_helper):
    seed(seeds)

    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00 UTC',
        'last_ingestion_timestamp': '2020-03-24 12:53:57.123 UTC',
        "first_eid": 'z/1970/01/01/00/00/0000000000000-0000001',
        "last_eid": 'z/2020/03/24/12/53/1585054437123-0000001'
    }

    sql = lake_to_staging.parse_template(
        f'{settings.DAGS_FOLDER}/templates/sql/lake_to_pii.users.sql', **checkpoint)
    print(sql)

    rs = bigquery_helper.query(sql)
    assert len(rs) == 1
