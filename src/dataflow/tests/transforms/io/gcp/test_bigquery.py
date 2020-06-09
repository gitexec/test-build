import pytest
import apache_beam as beam
from libs import GCLOUD as gcloud
from transforms.io.gcp import bigquery as bq
from google.cloud import bigquery as gcp_bq
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from libs.shared.test_factories import FactoryRegistry
from apache_beam.options.value_provider import RuntimeValueProvider
project_id = gcloud.project('local')
bucket = f'{project_id}-wrench-imports'


def test_get_bigquery_data_fn_init_with_empty_strings():
    ls = ['project', 'table', 'query']
    fn = bq.GetBigQueryDataFn(**{n: '' for n in ls})
    for f in ls:
        attr = getattr(fn, f'_{f}')
        assert attr.get() == ''


def test_get_bigquery_data_fn_init_page_size_is_int():
    fn = bq.GetBigQueryDataFn(page_size=100)
    assert fn._page_size.get() == 100


def test_get_bigquery_data_fn_init_with_test_value():
    ls = ['project', 'table', 'query']
    fn = bq.GetBigQueryDataFn(**{n: 'test' for n in ls})
    for f in ls:
        attr = getattr(fn, f'_{f}')
        assert attr.get() == 'test'


@pytest.fixture(scope='module')
def seed(bigquery):
    seeds = [('lake', [
        ('tree_user_types', []),
        ('pyr_rank_definitions', FactoryRegistry.registry['LakePyrRankDefinitionFactory'])])]
    bigquery.truncate(seeds)
    bigquery.seed(seeds)


def test_pass_query_not_project_fails():
    with pytest.raises(Exception) as exception:
        with TestPipeline() as p:
            (p | 'No Project for Query' >> bq.ReadAsJson(query='Test'))
    assert 'query requires project to be specified!' == str(exception.value)


def test_pass_query_and_table_fails():
    with pytest.raises(Exception) as exception:
        with TestPipeline() as p:
            (p | 'Query and Table Set' >> bq.ReadAsJson(query='Test', table='Test2'))
    assert 'Cannot use both table and query arguments!' == str(exception.value)


def test_pass_table_reads_from_table_without_project(seed, env):
    with TestPipeline() as p:
        pcoll = (p | 'Reading from table' >> bq.ReadAsJson(table=f'{env["project"]}.lake.pyr_rank_definitions')
                   | 'Get Rows Count' >> beam.combiners.Count.Globally())
        assert_that(pcoll, equal_to([16]))


def test_pass_query_reads_using_query_and_project(seed, env):
    with TestPipeline() as p:
        sql = """
        SELECT * FROM lake.pyr_rank_definitions WHERE icentris_client = "bluesun"
        """
        pcoll = (p | 'Reading from table' >> bq.ReadAsJson(query=sql, project=env['project'])
                   | 'Get Rows Count' >> beam.combiners.Count.Globally())
        assert_that(pcoll, equal_to([3]))


@pytest.mark.skip(reason='hard-coded to local')
def test_inserting_the_dest_table_schema_into_pcollection():
    with TestPipeline() as p:
        lake_table = f'{project_id}:lake.wrench_metrics'
        expected = [
            {
                'schema': [
                    gcp_bq.schema.SchemaField('entity_id', 'STRING', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('tree_user_id', 'INTEGER', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('prediction', 'STRING', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('client_wrench_id', 'STRING', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('expirement_name', 'STRING', 'NULLABLE', None, ()),
                    gcp_bq.schema.SchemaField('processing_datetime', 'DATETIME', 'NULLABLE', None, ()),
                    gcp_bq.schema.SchemaField('ingestion_timestamp', 'TIMESTAMP', 'REQUIRED', None, ())
                ],
                'payload':{}
            }]
        pcoll = p | beam.Create([{}])
        schema_pcoll = pcoll | beam.ParDo(bq.IngectTableSchema(table=lake_table))
        assert_that(schema_pcoll, equal_to(expected))


@pytest.mark.skip(reason='hard-coded to local')
def test_inserting_the_dest_table_schema_into_pcollection_runtime():
    with TestPipeline() as p:
        lake_table = RuntimeValueProvider(
            option_name='dest',
            value_type=str,
            default_value=f'{project_id}:lake.wrench_metrics'
        )
        expected = [
            {
                'schema': [
                    gcp_bq.schema.SchemaField('entity_id', 'STRING', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('tree_user_id', 'INTEGER', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('prediction', 'STRING', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('client_wrench_id', 'STRING', 'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('expirement_name', 'STRING', 'NULLABLE', None, ()),
                    gcp_bq.schema.SchemaField('processing_datetime', 'DATETIME', 'NULLABLE', None, ()),
                    gcp_bq.schema.SchemaField('ingestion_timestamp', 'TIMESTAMP', 'REQUIRED', None, ())
                ],
                'payload':{}
            }]
        pcoll = p | beam.Create([{}])
        schema_pcoll = pcoll | beam.ParDo(bq.IngectTableSchema(table=lake_table))
        assert_that(schema_pcoll, equal_to(expected))
        RuntimeValueProvider.set_runtime_options(None)
