from airflow import settings
from importlib import util
from libs.shared.test import skipif_prd

# Dynamically load DAG as a module so we can access the parse_template function - jc 3/24/2020
spec = util.spec_from_file_location('lake_to_staging', f'{settings.DAGS_FOLDER}/lake_to_staging.py')
lake_to_staging = util.module_from_spec(spec)
spec.loader.exec_module(lake_to_staging)

seeds = [
    ('lake', [
        ('users', [
            {'id': 1, 'icentris_client': 'worldventures', 'tree_user_id': 1, 'email': 'email@email.com',
             'encrypted_password': '', 'failed_attempts': 0,
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
            {'id': 1, 'icentris_client': 'bluesun', 'tree_user_id': 1, 'email': 'george@email.com',
             'encrypted_password': '', 'failed_attempts': 0,
             'leo_eid': 'z/2020/02/24/00/00/0000000000100-0000001', 'ingestion_timestamp': '2020-02-24 00:00:00.01 UTC'},
        ]),
        ('tree_users', [
            {'id': 1, 'icentris_client': 'worldventures', 'user_type_id': 1,
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
            {'id': 1, 'icentris_client': 'bluesun', 'user_type_id': 1,
             'leo_eid': 'z/2020/02/24/00/00/0000000000100-0000001', 'ingestion_timestamp': '2020-02-24 00:00:00.01 UTC'},
        ]),
        ('tree_user_types', [
            {'id': 1, 'icentris_client': 'worldventures', 'description': 'distributor',
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
            {'id': 1, 'icentris_client': 'bluesun', 'description': 'blue distributor',
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
        ]),
        ('tree_orders', [
            {
                'id': 1, 'icentris_client': 'worldventures', 'order_type_id': 1, 'order_status_id': 1,
                'tree_user_id': 1, 'shipping_city': 'Mordor', 'created_date': '1970-01-01T00:00:00',
                'order_date': '1970-01-01T00:00:00', 'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001',
                'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC',
             },
            {
                'id': 1, 'icentris_client': 'worldventures', 'order_type_id': 1, 'order_status_id': 1,
                'tree_user_id': 1, 'shipping_city': 'Death Star City', 'created_date': '1970-01-01T00:00:00',
                'order_date': '1970-01-01T00:00:00', 'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001',
                'ingestion_timestamp': '2020-03-24 12:53:57.123 UTC'
            },
            {
                'id': 1, 'icentris_client': 'bluesun', 'order_type_id': 1, 'order_status_id': 1,
                'tree_user_id': 1, 'shipping_city': 'Blueville', 'created_date': '2020-02-24T00:00:00',
                'order_date': '2020-02-24T00:00:00', 'leo_eid': 'z/2020/02/24/00/00/0000000000100-0000001',
                'ingestion_timestamp': '2020-02-24 12:53:57.123 UTC'
            },
        ]),
        ('tree_order_items', [
            {'id': 1, 'icentris_client': 'worldventures', 'order_item_id': 1, 'order_id': 1,
             'product_id': 1, 'product_code': '12345',
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
            {'id': 1, 'icentris_client': 'bluesun', 'order_item_id': 1, 'order_id': 1,
             'product_id': 1, 'product_code': 'blue-12345',
             'leo_eid': 'z/2020/02/24/00/00/0000000000100-0000001', 'ingestion_timestamp': '2020-02-24 00:00:00.01 UTC'},
        ]),
        ('tree_order_types', [
            {'id': 1, 'icentris_client': 'worldventures',
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
            {'id': 1, 'icentris_client': 'bluesun',
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
        ]),
        ('tree_order_statuses', [
            {'id': 1, 'icentris_client': 'worldventures', 'description': 'active',
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
            {'id': 1, 'icentris_client': 'bluesun', 'description': 'active',
             'leo_eid': 'z/1970/01/01/00/00/0000000000100-0000001', 'ingestion_timestamp': '1970-01-01 00:00:00.01 UTC'},
        ])
    ])]


@skipif_prd
def test_order_count(seed, bigquery_helper):
    seed(seeds)

    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00 UTC',
        'last_ingestion_timestamp': '2020-03-24 12:53:57.123 UTC',
        "first_eid": 'z/1970/01/01/00/00/0000000000000-0000001',
        "last_eid": 'z/2020/03/24/12/53/1585054437123-0000001'
    }

    sql = lake_to_staging.parse_template(
        f'{settings.DAGS_FOLDER}/templates/sql/lake_to_staging.orders.sql', **checkpoint)
    print(sql)

    rs = bigquery_helper.query(sql)
    assert len(rs) == 2
    wv = list(filter(lambda r: r['icentris_client'] == 'worldventures', rs))
    assert wv[0]['shipping_city'] == 'Death Star City'
