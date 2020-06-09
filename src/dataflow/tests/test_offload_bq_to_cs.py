import pytest
import os
import gzip
import json
from libs import GCLOUD as gcloud, BigQuery

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider

from offload_bq_to_cs import Runner, RuntimeOptions

bucket = f"{gcloud.project(os.environ['ENV'])}-dataflow"
blob_name = "offload_bq_to_cs_test"
suffix = '-00000-of-00001.ndjson.gz'
dest_blob_name = f'{blob_name}{suffix}'


@pytest.fixture(scope='module')
def ndjson(env, cloudstorage, record_testsuite_property):
    cloudstorage.client.delete_blob(bucket, dest_blob_name)
    assert cloudstorage.client.blob_exists(bucket, dest_blob_name) is False

    sql = BigQuery.querybuilder(
        union=('all', [
            BigQuery.querybuilder(select=[
                ('NULL', 'none'),
                ('True', 'true_bool'),
                ('False', 'false_bool'),
                ('"2020-04-03"', 'date'),
                ('"2020-04-03 13:45:00"', 'datetime'),
                ('"1966-06-06 06:06:06.666666 UTC"', 'timestamp'),
                ('"STRING"', 'string'),
                ('234', 'integer'),
                ('123.54', 'float')
            ]),
            BigQuery.querybuilder(select=['NULL'] * 9),
            BigQuery.querybuilder(select=[
                '"String"',
                'False',
                'True',
                '"1993-09-03"',
                '"1993-09-03 03:44:00"',
                '"1993-09-03 03:44:00.777555 UTC"',
                '"Not String"',
                '567',
                '456'
            ])
        ])
    )

    RuntimeValueProvider.set_runtime_options(None)

    options = RuntimeOptions(['--env', env['env'],  '--query', str(sql), '--destination', f'gs://{bucket}/{blob_name}'])
    Runner._run(TestPipeline(options=options), options)

    assert cloudstorage.client.blob_exists(bucket, dest_blob_name) is True

    zbytes = cloudstorage.client.download_blob_as_string(bucket, dest_blob_name)
    bytes = gzip.decompress(zbytes)
    lns = bytes.decode('utf8').rstrip().split('\n')
    yield [json.loads(l) for l in lns]


def test_file_has_3_rows(ndjson):
    assert len(ndjson) == 3


def test_int_is_int(ndjson):
    assert isinstance(ndjson[0]['integer'], int)
    assert isinstance(ndjson[2]['integer'], int)
    assert ndjson[0]['integer'] == 234
    assert ndjson[2]['integer'] == 567


def test_None_is_empty_string(ndjson):
    assert ndjson[0]['none'] == ''


def test_bool_false_is_0(ndjson):
    assert ndjson[0]['false_bool'] is not False and ndjson[0]['false_bool'] == 0
    assert ndjson[2]['true_bool'] is not False and ndjson[2]['true_bool'] == 0


def test_bool_true_is_1(ndjson):
    assert ndjson[0]['true_bool'] is not True and ndjson[0]['true_bool'] == 1
    assert ndjson[2]['false_bool'] is not True and ndjson[2]['false_bool'] == 1


def test_all_fields_null_empty_strings(ndjson):
    assert all([v == '' for k, v in ndjson[1].items()])
