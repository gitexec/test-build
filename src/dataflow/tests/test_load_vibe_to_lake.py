import pytest
from datetime import datetime
from libs.shared.test import skipif_prd

seeds = [
    ('bluesun', [
        ('tree_users', [
            {'id': 1, 'first_name': 'bob', 'last_name': 'saget', 'user_type_id': 1, 'user_status_id': 1,
             'created_date': '2018-01-01'},
            {'id': 2, 'first_name': 'jimmy', 'last_name': 'fallon', 'user_type_id': 1,
             'user_status_id': 1, 'created_date': '2017-01-01'},
            {'id': 4, 'first_name': 'chris', 'last_name': 'farley', 'user_type_id': 2,
             'user_status_id': 3, 'created_date': '1985-01-02'}])
    ]),
    ('lake', [
        ('tree_users', [
            # Placeholder
        ])
    ])
]


@pytest.fixture
def setup_and_teardown(bigquery):
    bigquery.truncate(seeds)
    bigquery.seed(seeds)
    yield
    bigquery.truncate(seeds)


@pytest.mark.skip(reason='Missing parse_template fixture. Needs template pre-launched; hard-coded to local')
@skipif_prd
def test_dataflow_job(setup_and_teardown, parse_template, run, bigquery):
    start = str(datetime.utcnow())
    end = str(datetime.utcnow())

    run('load_vibe_to_lake',
        client='bluesun',
        table='pyr_bluesun_local.tree_users',
        dest='<your-project-id-goes-here>:lake.tree_users')

    # Revisit query params later - Stu M. 4/1/20
    # job_cfg = bq.QueryJobConfig(
    #     query_parameters=[
    #         bq.ScalarQueryParameter('start', 'TIMESTAMP', start),
    #         bq.ScalarQueryParameter('end', 'TIMESTAMP', end)
    #     ]
    # )

    query = """
            SELECT * FROM lake.tree_users
            # WHERE leo_eid = 'z/0'
            # AND ingestion_timestamp BETWEEN '{}' AND '{}'
    """
    rs1 = bigquery.query(query.format(start, end))

    assert len(rs1) == 3
