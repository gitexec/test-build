import logging
import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.options.value_provider import StaticValueProvider, ValueProvider
from google.cloud import bigquery
from past.builtins import unicode


class GetBigQueryDataFn(beam.DoFn):
    def __init__(self, **kwargs):
        self._client = None
        for k, v in kwargs.items():
            if isinstance(v, (str, unicode, int)):
                v = StaticValueProvider(type(v), v)
            setattr(self, '_'+k, v)

    def start_bundle(self):
        project = None
        if self._project:
            project = self._project.get()
        self._client = bigquery.Client(project=project)
        logging.getLogger().info('>> client.project: %s' % self._client.project)

    def finish_bundle(self):
        self._client = None

    def process(self, element):
        if self._table is not None:
            qry = 'SELECT * FROM {}'.format(self._table.get())
        else:
            qry = self._query.get()
        logging.getLogger().info('>> query: %s' % qry)
        logging.getLogger().info('>> page_size: %d' % self._page_size.get())
        query_job = self._client.query(qry)
        rows = query_job.result(page_size=self._page_size.get())
        logging.getLogger().info('>> total_rows: %d' % rows.total_rows)

        for row in rows:
            yield dict(row.items())


class ReadAsJson(PTransform):
    def __init__(self, project=None, query=None, table=None, page_size=10000):
        if query and table:
            raise Exception('Cannot use both table and query arguments!')

        if query and not project:
            raise Exception('query requires project to be specified!')

        self.project = project
        self.table = table
        self.query = query
        self.page_size = page_size

    def expand(self, pcoll):
        return (pcoll
                | "Initializing with empty collection" >> beam.Create([1])
                | 'Read records from BigQuery' >> beam.ParDo(
                    GetBigQueryDataFn(
                        query=self.query,
                        table=self.table,
                        project=self.project,
                        page_size=self.page_size
                    ))
                )


class IngectTableSchema(beam.DoFn):
    def __init__(self, table):
        self._client = None
        self._table = table
        self._table_schema = None

    def process(self, element):
        def _schematize(el):
            if self._client is None:
                self._client = bigquery.Client()
            if isinstance(self._table, ValueProvider):
                self._table = self._table.get()

            table_id = self._table.replace(':', '.')
            if self._table_schema is None:
                self._table_schema = self._client.get_table(table_id).schema  # Make an API request.
            return {**{'schema': self._table_schema}, **{'payload': element}}
        yield _schematize(element)
