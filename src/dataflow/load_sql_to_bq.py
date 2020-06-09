from __future__ import print_function

import logging
import apache_beam as beam
import base64
import datetime
import json
import pymysql.cursors
import sys
import zlib
from libs import Config
from libs import GCLOUD as gcloud
from decimal import Decimal
from pycloudsqlproxy import connect as proxy_connect

from apache_beam.options.pipeline_options import DebugOptions, PipelineOptions, SetupOptions
from apache_beam.transforms import window
from apache_beam.options.value_provider import StaticValueProvider
from past.builtins import unicode

from google.cloud import bigquery


def isstr(s):
    return isinstance(s, str)


"""
Full Command to Deploy Template to DataFlow:

python load_sql_to_bq.py \
    --project icentris-ml \
    --staging_location gs://icentris-ml-dataflow/staging \
    --temp_location gs://icentris-ml-dataflow/tmp \
    --template_location gs://icentris-ml-dataflow/templates/load_sql_to_bq \
    --runner DataflowRunner \
    --setup_file ./setup.py
"""

"""
ReadFromRelationalDBFn from beam_nuggets that allows for template variables
"""


class ReadFromRelationalDBFn(beam.DoFn):
    def __init__(self, **kwargs):
        self._conn = None
        self._max_id = None
        self._current_id = 0
        for k, v in kwargs.items():
            if isinstance(v, (str, unicode)):
                v = StaticValueProvider(v)
            setattr(self, '_'+k, v)

    def start_bundle(self):
        self._ml_config = Config.get_config(gcloud.env(self._env.get()))
        self._client_name = self._client.get()
        self._client_config = self._ml_config['cloudsql'][self._client_name]
        if self._client_config['host'] != 'mysql':
            proxy_connect(self._ml_config['cloudsql'][self._client.get()]['host'])

    def finish_bundle(self):
        if self._conn is not None:
            self._conn.close()

    def process(self, element):
        tbl = self._table.get()
        key_field = self._key_field.get()
        batch_size = int(self._batch_size)

        def run_me():
            try:
                if self._conn is None:
                    if self._client_config['host'] == 'mysql':
                        host = self._client_config['host']
                    else:
                        host = '127.0.0.1'
                    self._conn = pymysql.connect(host=host,
                                                 user=self._client_config['user'],
                                                 password=self._client_config['password'],
                                                 db=self._client_config['db'],
                                                 cursorclass=pymysql.cursors.SSDictCursor)

                with self._conn.cursor() as cursor:
                    cursor.execute('SELECT MAX({}) AS max_id FROM {}'.format(key_field, tbl))
                    rs = cursor.fetchone()
                    self._max_id = rs['max_id'] if rs['max_id'] is not None else 0

                    logging.info('>>> load_sql_to_bq - fetching from {} with max_id {}'.format(tbl, key_field))
                    while self._current_id < self._max_id:
                        if (self._current_id + batch_size) < self._max_id:
                            top = self._current_id + batch_size
                        else:
                            top = self._max_id

                        logging.info('>>> load_sql_to_bq ReadFromRelationalDB - fetching batch {} to {}'
                                     .format(self._current_id, top))
                        cursor.execute('SELECT * FROM {} WHERE {} BETWEEN {} AND {}'
                                       .format(tbl, key_field, self._current_id, top))

                        r = cursor.fetchone()
                        while r:
                            yield r
                            r = cursor.fetchone()

                        self._current_id = top+1
            except Exception as e:
                nonlocal tick
                logging.error('>>> load_sql_to_bq ReadFromRelationalDB - exception raised')
                if tick > 0 and 'connection refused' in e.args[1].lower():
                    logging.error('>>> load_sql_to_bq ReadFromRelationalDB connection refused trying again')
                    tick -= 1
                    if self._client_config['host'] != 'mysql':
                        proxy_connect(self._client_config['host'])
                    run_me()
                else:
                    raise e

        tick = 5
        yield from run_me()


class BuildForBigQueryFn(beam.DoFn):
    def process(self, element):
        for f, v in element.items():
            if isinstance(v, Decimal):
                element[f] = str(v)
            elif isstr(v) and '0000-00-00' in v:
                element[f] = None
            elif isinstance(v, datetime.datetime):
                element[f] = str(v)
            elif isinstance(v, datetime.date):
                element[f] = str(v)
            elif isstr(v) and len(v) > 1000:
                v = base64.b64encode(zlib.compress(v.encode('utf-8'))).decode('ascii')
                element[f] = v

        logging.getLogger().info('>>>> processing element')
        logging.getLogger().info(element)
        yield element


"""
GroupIntoBatches (because Google DataFlow doesn't support state)
"""


class GroupIntoBatches(beam.DoFn):
    def __init__(self, batch_size):
        self._batch_size = batch_size
        self._max_batch_length = 8000000
        self._current_batch_length = 0

    def start_bundle(self):
        self._batch = []

    def process(self, element):
        self._batch.append(element)
        character_length = element.pop('dataflow_element_character_length', 0)
        self._current_batch_length += character_length

        if (len(self._batch) >= int(self._batch_size.get()) or
                (self._current_batch_length + character_length) >= self._max_batch_length):
            logging.info('>>> load_sql_to_bq GroupIntoBatches - current_batch_length + character_length {}'
                         .format(self._current_batch_length + character_length))
            logging.info('>>> load_sql_to_bq GroupIntoBatches - {} records'.format(len(self._batch)))
            yield self._batch
            self._batch = []
            self._current_batch_length = 0

    def finish_bundle(self):
        if len(self._batch) != 0:
            yield window.GlobalWindows.windowed_value(self._batch)
        self._batch = []


"""
WriteToBigQuery with runtime values
"""


class WriteToBigQueryFn(beam.DoFn):
    def __init__(self, project, env, client, table, schema):
        self._project = project
        self._env = env
        self._client = client
        self._table = table
        self._schema = schema
        self._initialized = False
        self._dataset_ref = None
        self._table_ref = None

    def process(self, element):
        if self._initialized is False:
            logging.info('>>> load_sql_to_bq WriteToBigQuery - loading schema list')
            schema = []
            loaded = json.loads(self._schema.get())
            for f in loaded['fields']:
                schema.append(bigquery.SchemaField(f['name'], f['type'], f['mode']))
            self._schema = schema

            logging.info('>>> load_sql_to_bq WriteToBigQuery - initializing client, refs, data set and table')
            dataset = 'pyr_{}_{}'.format(self._client.get(), self._env.get())
            self._client = bigquery.Client(project=self._project)
            self._dataset_ref = self._client.dataset(dataset)
            self._table_ref = self._dataset_ref.table(self._table.get())

            try:
                # Delete the table if it exists - jc 2/19/20
                self._client.delete_table(self._table_ref)
            except Exception:
                pass

            self._client.create_dataset(self._dataset_ref, exists_ok=True)
            self._client.create_table(bigquery.Table(self._table_ref, schema=self._schema))
            self._initialized = True

        logging.info('>>> load_sql_to_bq WriteToBigQuery - writing {} records to bigquery'.format(len(element)))
        logging.info('>>> load_sql_to_bq WriteToBigQuery - element len is {}'
                     .format(len(json.dumps(element, default=str))))
        rs = self._client.insert_rows(
            self._table_ref,
            element,
            self._schema)
        logging.info('>>> load_sql_to_bq WriteToBigQuery - committed {} records to bigquery'.format(len(element)))
        logging.info(rs)


class LoadSqlToBqOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--client', help='monat, idlife, stampinup')
        parser.add_value_provider_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--table', help='mysql table')
        parser.add_value_provider_argument('--key_field', help='primary key field')
        parser.add_value_provider_argument('--bq_table', help='fully qualified bq table identifier')


class Runner():
    def run(argv=None):
        log = logging.getLogger()
        log.warning('>> Running cloudsql to bigquery pipeline')

        options = LoadSqlToBqOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']
        custom_options = options.view_as(LoadSqlToBqOptions)

        with beam.Pipeline(options=options) as p:
            (p
                | "Initializing with empty collection" >> beam.Create([1])
                | "Reading records from CloudSql" >> beam.ParDo(ReadFromRelationalDBFn(
                    env=custom_options.env,
                    client=custom_options.client,
                    table=custom_options.table,
                    key_field=custom_options.key_field,
                    batch_size=10000))
                | "Converting Row Object for BigQuery" >> beam.ParDo(BuildForBigQueryFn())
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    custom_options.bq_table,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
                )
             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_sql_to_bq - Starting DataFlow Pipeline Runner')
    Runner().run()
