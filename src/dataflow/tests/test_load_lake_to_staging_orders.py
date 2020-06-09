import pytest
import datetime
from libs.shared.test import skipif_prd
from libs.shared.utils import parse_template
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import LakeTreeUserFactory, LakeTreeOrderFactory
from load_lake_to_staging_orders import Runner, RuntimeOptions


@pytest.fixture(scope='module')
def run(env, run_pipeline, sql_templates_path, bigquery):
    start = datetime.datetime.now()
    end = start+datetime.timedelta(seconds=1)

    start = start.strftime('%Y-%m-%d %H:%M:%S') + ' UTC'
    end = end.strftime('%Y-%m-%d %H:%M:%S') + ' UTC'

    FactoryRegistry.create_multiple(LakeTreeUserFactory, 2, [
        {'icentris_client': 'bluesun'},
        {'icentris_client': 'worldventures'}
    ])
    FactoryRegistry.create_multiple(LakeTreeOrderFactory, 2, [
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][0]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][0]['icentris_client'],
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': f'{str(start)} UTC'},
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][1]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][1]['icentris_client'],
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': f'{str(start)} UTC'},
    ])

    seeds = [
        ('lake', [
            ('tree_users', FactoryRegistry.registry['LakeTreeUserFactory']),
            ('tree_user_types', FactoryRegistry.registry['LakeTreeUserTypeFactory']),
            ('tree_user_statuses', FactoryRegistry.registry['LakeTreeUserStatusFactory']),
            ('tree_orders', FactoryRegistry.registry['LakeTreeOrderFactory']),
            ('tree_order_types', FactoryRegistry.registry['LakeTreeOrderTypeFactory']),
            ('tree_order_statuses', FactoryRegistry.registry['LakeTreeOrderStatusFactory'])
        ])]

    checkpoint = {
        'first_ingestion_timestamp': start,
        'last_ingestion_timestamp': end
    }

    sql = parse_template(
                f'{sql_templates_path}/lake_to_staging.orders.sql', **checkpoint)
    # print(sql)
    run_pipeline(
        Runner,
        RuntimeOptions([
            '--env',
            env['env'],
            '--query',
            sql]),
        seeds)

    rs = bigquery.query(f'select * from staging.orders WHERE ingestion_timestamp >= "{start}"')
    return rs


@skipif_prd
def test_loads_2_records_into_lake_orders(run):
    assert len(run) == 2
