from __future__ import print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, DebugOptions, SetupOptions
import logging
import sys
from transforms.io.gcp import bigquery as bq
from libs import GCLOUD
from transforms.datetime import InsertIngestionTimestamp, StringifyDatetimes
from transforms.worldventures import WorldVenturesStagingContactsTransform


class Bluesun(beam.DoFn):
    def process(self, element):
        icentris_client = element['icentris_client'].lower()
        if icentris_client == 'bluesun':
            element['type'] = element['client_type']
            yield element


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', default='local', help='local, dev, prd')
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
            staging_table = 'staging.contacts'
            big_query_data = (p | 'Read BigQuery Data' >> bq.ReadAsJson(
                project=project_id,
                query=options.query,
                page_size=options.page_size)

                                   | 'Insert Ingestion Timestamp' >> beam.ParDo(InsertIngestionTimestamp())
                                   | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes()))

            wv_p = (big_query_data | 'Apply World Ventures Transform' >> WorldVenturesStagingContactsTransform())

            bs_p = (big_query_data | 'Apply Bluesun Transform' >> beam.ParDo(Bluesun()))

            ((wv_p, bs_p) | 'Merge Client Collections for Writing to BigQuery' >> beam.Flatten()
                | 'Write to staging' >> beam.io.WriteToBigQuery(
                    '{}:{}'.format(project_id, staging_table),
                    insert_retry_strategy='RETRY_NEVER',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running BigQuery to Cloud Storage pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']
        options.view_as(StandardOptions).streaming = False

        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_lake_to_staging_contacts - Starting DataFlow Pipeline Runner')
    Runner.run()
