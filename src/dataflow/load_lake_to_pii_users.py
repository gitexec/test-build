from __future__ import print_function

import logging
import apache_beam as beam
import sys
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, DebugOptions, SetupOptions

from transforms.io.gcp import bigquery as bq
from libs import GCLOUD
from transforms.datetime import InsertIngestionTimestamp, StringifyDatetimes


class WorldVentures(beam.DoFn):
    def process(self, payload):
        icentris_client = payload['icentris_client'].lower()
        if icentris_client == 'worldventures':
            yield payload


class Bluesun(beam.DoFn):
    def process(self, element):
        icentris_client = element['icentris_client'].lower()
        if icentris_client == 'bluesun':
            yield element


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--query', type=str, help='BigQuery query statement')
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    @classmethod
    def _run(cls, p, options):
        with p:
            project_id = GCLOUD.project(options.env)

            big_query_data = (p | 'Read BigQuery Data' >> bq.ReadAsJson(
                project=project_id,
                query=options.query,
                page_size=options.page_size)
                                | 'Insert Ingestion Timestamp' >> beam.ParDo(InsertIngestionTimestamp())
                                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes()))

            staging_table = 'pii.users'

            wv_p = (big_query_data | 'Apply World Ventures Transform' >> beam.ParDo(WorldVentures()))

            bs_p = (big_query_data | 'Apply Bluesun Transform' >> beam.ParDo(Bluesun()))

            ((wv_p, bs_p) | 'Merge Client Collections for Writing to BigQuery' >> beam.Flatten()
                | 'Write to Bluesun staging' >> beam.io.WriteToBigQuery(
                    '{}:{}'.format(project_id, staging_table),
                    insert_retry_strategy='RETRY_NEVER',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running Lake to PII Users pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']
        options.view_as(StandardOptions).streaming = False

        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_lake_to_pii_users - Starting DataFlow Pipeline Runner')
    Runner().run()
