from __future__ import print_function

import logging
import apache_beam as beam
import json
import sys
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import transforms.io.gcp.bigquery as bq
from transforms.datetime import StringifyDatetimes
from libs import GCLOUD as gcloud


class ConvertNoneToEmptyString(beam.DoFn):
    def process(self, element):
        for k, v in element.items():
            if element[k] is None:
                element[k] = ''
        yield element


class ConvertBoolsToInts(beam.DoFn):
    def process(self, element):
        for k, v in element.items():
            if isinstance(element[k], bool):
                element[k] = element[k]*1
        yield element


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument(
            '--query',
            help='BigQuery query for source data (e.g., SELECT * FROM `dataset_id.table`)')
        parser.add_value_provider_argument(
            '--destination',
            help='Output file path (e.g., gs://my-bucket/my-output-file)')
        parser.add_value_provider_argument(
            '--page-size',
            type=int,
            default=10000,
            help='Page size for BigQuery results')


class Runner():

    @classmethod
    def _run(cls, p, options):
        project_id = gcloud.project(options.env)
        with p:
            (p
                | 'Read BigQuery Data' >> bq.ReadAsJson(
                    project=project_id,
                    query=options.query,
                    page_size=options.page_size)
                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes())
                | 'Convert None values to empty strings' >> beam.ParDo(ConvertNoneToEmptyString())
                | 'Convert Bool values to ints' >> beam.ParDo(ConvertBoolsToInts())
                | 'Convert dict elements to json' >> beam.Map(json.dumps)
                | 'Write file to Cloud Storage' >> beam.io.WriteToText(
                    options.destination,
                    file_name_suffix='.ndjson.gz',
                    compression_type=beam.io.filesystem.CompressionTypes.GZIP
                )
             )

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running BigQuery to Cloud Storage pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True

        """
        beam.io.BigQuerySource does not support ValueProvider (in this case, the 'query' arg). Until that happens we're
        forced to implement a customer PTransform that can execute a query that was passed in as an argument.
        10/7/2019 - jc
        """
        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().info('> offload_bq_to_cs - Starting DataFlow Pipeline Runner')
    Runner.run()
