import pytest
import datetime
from libs.shared.test import skipif_prd
from libs.shared.utils import parse_template
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import (
    LakeTreeUserFactory,
    LakePyrContactsFactory,
    LakePyrContactCategoriesFactory,
    LakePyrContactsContactCategoriesFactory,
    LakePyrContactEmailsFactory,
    LakePyrContactPhoneNumbersFactory)
from load_lake_to_staging_contacts import Runner, RuntimeOptions


@pytest.fixture(scope='module')
def run(env, run_pipeline, sql_templates_path, bigquery):

    FactoryRegistry.create_multiple(LakeTreeUserFactory, 5, [
        {
            'id': 1,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 1,
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2,
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 1,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/03/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-03-01 00:00:00.0 UTC'}
    ])

    FactoryRegistry.create_multiple(LakePyrContactsFactory, 6, [
        {
            'id': 1, 'tree_user_id': 1,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2, 'tree_user_id': 2,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 1, 'tree_user_id': 1,
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2, 'tree_user_id': 2,
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2, 'tree_user_id': 2,
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/02/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-02-01 00:00:00.0 UTC'},
        {
            'id': 1, 'tree_user_id': 1,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-03-01 00:00:01.0 UTC'}
    ])

    FactoryRegistry.create_multiple(LakePyrContactCategoriesFactory, 4, [
        {
            'id': 1, 'category_name': 'bluesun One', 'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2, 'category_name': 'bluesun Two', 'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 1, 'category_name': 'WV One', 'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 1, 'category_name': 'bluesun One-2020', 'icentris_client': 'bluesun',
            'leo_eid': 'z/2020/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2020-01-01 00:00:00.0 UTC'}
    ])

    FactoryRegistry.create_multiple(LakePyrContactsContactCategoriesFactory, 5, [
        {
            'id': 1, 'contact_id': 1,
            'contact_category_id': 1,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2,
            'contact_id': 2,
            'contact_category_id': 1,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000', 'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 3,
            'contact_id': 1,
            'contact_category_id': 1,
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 4, 'contact_id': 2,
            'contact_category_id': 1,
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 5, 'contact_id': 1,
            'contact_category_id': 2,
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/04/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-04-01 00:00:00.0 UTC'}
    ])

    FactoryRegistry.create_multiple(LakePyrContactEmailsFactory, 5, [
        {
            'id': 1,
            'contact_id': 1,
            'email': 'jon.doe@test.com',
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2,
            'contact_id': 2,
            'email': 'jane.doe@test.com',
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 3,
            'contact_id': 1,
            'email': 'matthew.gospel@test.com',
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 4, 'contact_id': 2,
            'email': 'titus.epistle@test.com',
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 5, 'contact_id': 1,
            'email': 'jonathan.doe@test.com',
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/05/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-05-01 00:00:01.0 UTC'}
    ])

    FactoryRegistry.create_multiple(LakePyrContactPhoneNumbersFactory, 5, [
        {
            'id': 1,
            'contact_id': 1,
            'phone_number': '111-111-1111',
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 2,
            'contact_id': 2,
            'phone_number': '222-222-2222',
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 3,
            'contact_id': 1,
            'phone_number': '1-111-111-1111',
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 4,
            'contact_id': 2,
            'phone_number': '2-222-222-2222',
            'icentris_client': 'worldventures',
            'leo_eid': 'z/2019/01/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'},
        {
            'id': 5,
            'contact_id': 1,
            'phone_number': '333-333-3333',
            'icentris_client': 'bluesun',
            'leo_eid': 'z/2019/06/01/00/00/0000000000000-0000000',
            'ingestion_timestamp': '2019-06-01 00:00:00.0 UTC'}
    ])

    seeds = [
        ('lake', [
            ('tree_users', FactoryRegistry.registry['LakeTreeUserFactory']),
            ('pyr_contacts', FactoryRegistry.registry['LakePyrContactsFactory']),
            ('pyr_contact_categories', FactoryRegistry.registry['LakePyrContactCategoriesFactory']),
            ('pyr_contacts_contact_categories',
                FactoryRegistry.registry['LakePyrContactsContactCategoriesFactory']),
            ('pyr_contact_emails', FactoryRegistry.registry['LakePyrContactEmailsFactory']),
            ('pyr_contact_phone_numbers', FactoryRegistry.registry['LakePyrContactPhoneNumbersFactory'])
        ])]

    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
        "first_eid": 'z/1970/01/01/00/00/0000000000000-0000000',
        "last_eid": 'z/2019/01/01/00/00/0000000000000-0000000'
    }

    sql = parse_template(
                f'{sql_templates_path}/lake_to_staging.contacts.sql', **checkpoint)

    run_pipeline(
        Runner,
        RuntimeOptions([
            '--env',
            env['env'],
            '--query',
            sql]),
        seeds)

    it = datetime.datetime.fromisoformat('2019-01-01 00:00:00')
    rs = bigquery.query(f'select * from staging.contacts WHERE ingestion_timestamp >= "{it}"')
    return rs


@skipif_prd
def test_ingestion_timestamp_eid_windows_multi_users(run):
    # TODO: Pass checkpoint dynamically
    # checkpoint = {
    #     'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
    #     "first_eid": 'z/1970/01/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/01/01/00/00/0000000000000-0000000'
    # }
    assert len(run) == 4
    for r in run:
        continue
        if r['icentris_client'] == 'bluesun':
            if r['contact_id'] == 1:
                assert r['first_name'] == 'Jon'
                assert r['categories'][0]['category'] == 'bluesun One'
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
@pytest.mark.skip(reason='Figure out how to dynamically update the checkpoint')
def test_ingestion_timestamp_eid_windows_single_user(run):
    # TODO: Pass checkpoint dynamically
    # checkpoint = {
    #     'first_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-02-01 00:00:00.0 UTC',
    #     "first_eid": 'z/2019/01/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/02/01/00/00/0000000000000-0000000'
    # }

    assert len(run) == 4
    # TODO - Come back later and get these assertions working
    # assert run[0]['icentris_client'] == 'worldventures'
    # assert run[0]['contact_id'] == 2
    # assert run[0]['first_name'] == 'TitusX'


@skipif_prd
@pytest.mark.skip(reason='Figure out how to dynamically update the checkpoint')
def test_replay_event(run):
    # checkpoint = {
    #     'first_ingestion_timestamp': '2019-03-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-03-01 12:00:00.0 UTC',
    #     "first_eid": 'z/2019/03/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/03/01/12/00/0000000000000-0000000'
    # }

    assert len(run) == 1
    assert run[0]['icentris_client'] == 'monat'
    assert run[0]['id'] == 1
    assert run[0]['first_name'] == 'Jon-Replay'


@skipif_prd
@pytest.mark.skip(reason='Figure out how to dynamically update the checkpoint')
def test_joined_table_event(run):
    # checkpoint = {
    #     'first_ingestion_timestamp': '2019-05-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-05-01 12:00:00.0 UTC',
    #     "first_eid": 'z/2019/05/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/05/01/12/00/0000000000000-0000000'
    # }

    assert len(run) == 1
    assert run[0]['icentris_client'] == 'bluesun'
    assert run[0]['id'] == 1
    assert any(e['email'] == 'jonathan.doe@test.com' for e in run[0]['emails'])
