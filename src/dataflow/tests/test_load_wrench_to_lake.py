import pytest
import apache_beam as beam
from libs import GCLOUD as gcloud
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from load_wrench_to_lake import WrenchCSVPayloadMapper, Runner, RuntimeOptions
import datetime

project_id = gcloud.project('local')
bucket = f'{project_id}-wrench-imports'
file_seed = """
entity_id,tree_user_id,prediction,owner,experiment_name,processing_datetime
1fab9649-5113-4daa-a9ed-ae67bc3358b8,1038665,low,e08dc822-97b1-46f4-9154-25821286231f,lcv_level,2020-01-31T14:52:40.000+05:30
c104b7ea-d32c-4c6e-92a5-505b3651d424,873612,zero,e08dc822-97b1-46f4-9154-25821286231f,lcv_level,2020-01-31T14:52:40.000+05:30
"""
filename = 'wrench_test.csv'


@pytest.fixture(scope='module')
def run(env, run_pipeline, bigquery, cloudstorage):
    start = datetime.datetime.now()
    lake_table = 'lake.wrench_metrics'
    files_ext = '.csv'

    [b.delete() for b in cloudstorage.client.list_blobs(bucket)]
    cloudstorage.client.upload_blob_from_string(bucket, file_seed, filename, 'text/csv')
    run_pipeline(
        Runner,
        RuntimeOptions([
            '--env',
            env['env'],
            '--files_ext',
            files_ext]),
        [])
    rs = bigquery.query(f'SELECT * FROM {lake_table} WHERE ingestion_timestamp >= "{start}"')
    return rs


@pytest.mark.skip(reason='hard-coded to local')
def test_wrench_csv_payload_mapper():
    seed = '1fab9649-5113-4daa-a9ed-ae67bc3358b8,\
    1038665,low,e08dc822-97b1-46f4-9154-25821286231f,lcv_level,2020-01-31T14:52:40.000+05:30'
    with TestPipeline() as p:
        expected = [{
            'entity_id': '1fab9649-5113-4daa-a9ed-ae67bc3358b8',
            'tree_user_id': 1038665,
            'prediction': 'low',
            'client_wrench_id': 'e08dc822-97b1-46f4-9154-25821286231f',
            'expirement_name': 'lcv_level',
            'processing_datetime': '2020-01-31T14:52:40.000+05:30'
        }]
        pcoll = p | beam.Create([seed])
        p_cvs = pcoll | beam.ParDo(WrenchCSVPayloadMapper('local'))
        assert_that(p_cvs, equal_to(expected))


def test_the_df_template_load_and_insert_two_records_into_bq(run):
    assert len(run) == 2
