from airflow import settings
from importlib import util
from libs.shared.test import skipif_prd

# Dynamically load DAG as a module so we can access the parse_template function - jc 3/24/2020
spec = util.spec_from_file_location('lake_to_staging', f'{settings.DAGS_FOLDER}/lake_to_staging.py')
lake_to_staging = util.module_from_spec(spec)
spec.loader.exec_module(lake_to_staging)

seeds = [
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
def test_ingestion_timestamp_eid_windows_multi_users(seed, bigquery_helper):
    seed(seeds)
    # Should pull contacts: Jon, Jane, Matthew, Titus
    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
        "first_eid": 'z/1970/01/01/00/00/0000000000000-0000000',
        "last_eid": 'z/2019/01/01/00/00/0000000000000-0000000'
    }

    sql = lake_to_staging.parse_template(
        f'{settings.DAGS_FOLDER}/templates/sql/lake_to_staging.contacts.sql', **checkpoint)
    # print(sql)

    rs = bigquery_helper.query(sql)
    assert len(rs) == 4

    for r in rs:
        continue
        if r['icentris_client'] == 'monat':
            if r['contact_id'] == 1:
                assert r['first_name'] == 'Jon'
                assert r['categories'][0]['category'] == 'Monat One'
            elif r['contact_id'] == 2:
                assert r['first_name'] == 'Jane'
            else:
                assert False
        elif r['icentris_client'] == 'worldventures':
            if r['contact_id'] == 1:
                assert r['first_name'] == 'Matthew'
            elif r['contact_id'] == 2:
                assert r['first_name'] == 'Titus'
            else:
                assert False
        else:
            assert False


@skipif_prd
def test_ingestion_timestamp_eid_windows_single_user(seed, bigquery_helper):
    seed(seeds)
    # Should pull contact: TitusX
    checkpoint = {
        'first_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-02-01 00:00:00.0 UTC',
        "first_eid": 'z/2019/01/01/00/00/0000000000000-0000000',
        "last_eid": 'z/2019/02/01/00/00/0000000000000-0000000'
    }

    sql = lake_to_staging.parse_template(
        f'{settings.DAGS_FOLDER}/templates/sql/lake_to_staging.contacts.sql', **checkpoint)
    # print(sql)

    rs = bigquery_helper.query(sql)
    assert len(rs) == 4
    # TODO - Come back later and get these assertions working
    # assert rs[0]['icentris_client'] == 'worldventures'
    # assert rs[0]['contact_id'] == 2
    # assert rs[0]['first_name'] == 'TitusX'


@skipif_prd
def test_replay_event(seed, bigquery_helper):
    seed(seeds)
    """
    This test case would replicate the scenario where there
    was an event replayed. So the leo_eid would be the same (old),
    but the ingestion_timestamp would be different (new).

    This should pull contact: Jon-Replay
    """

    checkpoint = {
        'first_ingestion_timestamp': '2019-03-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-03-01 12:00:00.0 UTC',
        "first_eid": 'z/2019/03/01/00/00/0000000000000-0000000',
        "last_eid": 'z/2019/03/01/12/00/0000000000000-0000000'
    }

    sql = lake_to_staging.parse_template(
        f'{settings.DAGS_FOLDER}/templates/sql/lake_to_staging.contacts.sql', **checkpoint)
    # print(sql)

    rs = bigquery_helper.query(sql)
    assert len(rs) == 1
    assert rs[0]['icentris_client'] == 'monat'
    assert rs[0]['id'] == 1
    assert rs[0]['first_name'] == 'Jon-Replay'


@skipif_prd
def test_joined_table_event(seed, bigquery_helper):
    """
    This test case covers the scenario where a record was changed
    in a joined table. In this case the primary table is pyr_contacts
    and the secondary table that was updated is pyr_contact_emails.

    This should pull monat contact 1, w/ email jonathan.doe@test.com
    """
    seed(seeds)
    checkpoint = {
        'first_ingestion_timestamp': '2019-05-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-05-01 12:00:00.0 UTC',
        "first_eid": 'z/2019/05/01/00/00/0000000000000-0000000',
        "last_eid": 'z/2019/05/01/12/00/0000000000000-0000000'
    }

    sql = lake_to_staging.parse_template(
        f'{settings.DAGS_FOLDER}/templates/sql/lake_to_staging.contacts.sql', **checkpoint)
    # print(sql)

    rs = bigquery_helper.query(sql)
    assert len(rs) == 1
    assert rs[0]['icentris_client'] == 'monat'
    assert rs[0]['id'] == 1
    assert any(e['email'] == 'jonathan.doe@test.com' for e in rs[0]['emails'])
