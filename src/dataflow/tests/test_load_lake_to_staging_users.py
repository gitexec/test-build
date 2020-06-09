import pytest
import datetime
from libs.shared.test import skipif_prd
from libs.shared.utils import parse_template
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import LakeTreeUserFactory, LakeUserFactory
from load_lake_to_staging_users import Runner, RuntimeOptions


@pytest.fixture(scope='module')
def run(env, run_pipeline, sql_templates_path, bigquery):
    start = datetime.datetime.now()
    end = start+datetime.timedelta(seconds=1)

    start = start.strftime('%Y-%m-%d %H:%M:%S') + ' UTC'
    end = end.strftime('%Y-%m-%d %H:%M:%S') + ' UTC'

    FactoryRegistry.create_multiple(LakeTreeUserFactory, 2, [
        {'icentris_client': 'bluesun',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': f'{str(start)} UTC'},
        {'icentris_client': 'worldventures',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': f'{str(start)} UTC'}
    ])
    FactoryRegistry.create_multiple(LakeUserFactory, 2, [
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][0]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][0]['icentris_client']},
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][1]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][1]['icentris_client']},
    ])

    seeds = [
        ('lake', [
            ('tree_users', FactoryRegistry.registry['LakeTreeUserFactory']),
            ('users', FactoryRegistry.registry['LakeUserFactory']),
            ('tree_user_types', FactoryRegistry.registry['LakeTreeUserTypeFactory']),
            ('pyr_rank_definitions', FactoryRegistry.registry['LakePyrRankDefinitionFactory']),
            ('tree_user_statuses', FactoryRegistry.registry['LakeTreeUserStatusFactory'])
        ])]

    checkpoint = {
        'first_ingestion_timestamp': start,
        'last_ingestion_timestamp': end
    }

    sql = parse_template(
                f'{sql_templates_path}/lake_to_staging.users.sql', **checkpoint)

    run_pipeline(
        Runner,
        RuntimeOptions([
            '--env',
            env['env'],
            '--query',
            sql]),
        seeds)

    rs = bigquery.query(f'select * from staging.users WHERE ingestion_timestamp >= "{start}"')
    return rs


@skipif_prd
def test_loads_2_records_into_lake_users(run):
    assert len(run) == 2
