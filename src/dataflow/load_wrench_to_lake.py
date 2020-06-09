from __future__ import print_function

import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, StandardOptions, SetupOptions
from transforms.io.gcp import bigquery as bq
from transforms.datetime import InsertIngestionTimestamp, StringifyDatetimes, StringToDatetime
from libs import GCLOUD
from transforms.io.filelist_iterator import FileListIteratorTransform
import dill


class WrenchCSVPayloadMapper(beam.DoFn):
    def process(self, lines):
        splits = lines.split(',')
        # https://stackoverflow.com/questions/50855647/skip-header-while-reading-a-csv-file-in-apache-beam
        # Skip first line of in the csv file. - Stu M 4/2/20
        if splits[0] != 'entity_id':
            payload = {
                'entity_id': splits[0],
                'tree_user_id': int(splits[1]),
                'prediction': splits[2],
                'client_wrench_id': splits[3],
                'expirement_name': splits[4],
                'processing_datetime': splits[5]
            }
            yield payload


# Note: Update sort_key function based on the filename format  wbrito 05/05/2020
class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument(
            '--files_ext',
            default='.csv',
            help='files extension'
            )
        parser.add_value_provider_argument(
            '--sort_key',
            default=bytes.hex(
                dill.dumps(
                    lambda f: int(f[f.rfind('*') + 1:]) if f[f.rfind('*') + 1:].isdigit() else f)),
            help='serialized function to sort file list'
            )
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    @classmethod
    def _run(cls, p, options):
        with p:
            project_id = GCLOUD.project(options.env)
            bucket = f'{project_id}-wrench-imports'
            dest_tbl = f'{project_id}:lake.wrench_metrics'
            (p
                | 'Iterate File Paths' >> FileListIteratorTransform(
                    env=options.env,
                    bucket=bucket,
                    files_ext=options.files_ext,
                    sort_key=options.sort_key)
                | 'Read from a File' >> beam.io.ReadAllFromText(skip_header_lines=1)
                | 'Apply Wrench Transform' >> beam.ParDo(WrenchCSVPayloadMapper())
                | 'Insert Ingestion Timestamp' >> beam.ParDo(InsertIngestionTimestamp())
                | 'Ingest table schema' >> beam.ParDo(bq.IngectTableSchema(table=dest_tbl))
                | 'Transform String to Standard SQL Datetime' >> beam.ParDo(StringToDatetime())
                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes('%Y-%m-%d %H:%M:%S'))
                | 'Write to lake' >> beam.io.WriteToBigQuery(
                    dest_tbl,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
                ))

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running Wrench to Lake Pipeline')

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
    logging.getLogger().warning('> load_wrench_to_lake - Starting DataFlow Pipeline Runner')
    Runner.run()
