from __future__ import print_function

import logging
import apache_beam as beam
import sys
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, SetupOptions
from transforms.io.gcp import bigquery
from transforms.datetime import StringifyDatetimes


class EnrichRecord(beam.DoFn):
    def __init__(self, client):
        self._client = client

    def process(self, element):
        enriched = {**element, **{'icentris_client': self._client.get(),
                                  'leo_eid': 'z/0',
                                  'ingestion_timestamp': str(datetime.utcnow())}}
        yield enriched


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--client', help='iCentris client key ex. monat, worldventures, avon')
        parser.add_value_provider_argument('--table', help='Table to load from')
        parser.add_value_provider_argument('--dest', help='Table to write to')
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    def run(self, argv=None):
        log = logging.getLogger()
        log.info('>> Running BigQuery to Cloud Storage pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']

        """
        beam.io.BigQuerySource does not support ValueProvider (in this case, the 'query' arg). Until that happens we're
        forced to implement a customer PTransform that can execute a query that was passed in as an argument.
        10/7/2019 - jc
        """
        with beam.Pipeline(options=options) as p:
            (p
                | 'Read BigQuery Data' >> bigquery.ReadAsJson()
                | 'Add Values for icentris_client, leo_id, ingestion_timestamp' >> beam.ParDo(
                    EnrichRecord(client=options.client))
                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes())
                | 'Write vibe table to lake' >> beam.io.WriteToBigQuery(
                    options.dest,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
                )
             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_vibe_to_lake - Starting DataFlow Pipeline Runner')
    Runner().run()
