from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class BigQueryMigration():
    def __init__(self, client):
        self.client = client
        self.default_clustering_fields = ['leo_eid:STRING',  'ingestion_timestamp:TIMESTAMP']

    def dataset(self, dataset):
        return bigquery.Dataset('{}.{}'.format(self.client.project, dataset))

    def create_dataset(self, dataset):
        dataset = self.dataset(dataset)
        dataset.location = 'us-west3'
        self.client.create_dataset(dataset)
        return dataset

    def create_table(self, name, project, dataset, schema=None, partition=None, clustering_fields=None):
        tbl = dataset.table(name)

        if clustering_fields:
            fields = [f.name for f in schema]
            if not partition:
                raise NameError('time_partitioning must be set to True when using clustering_fields.')
            if not schema:
                raise NameError('schema is required.')
            if partition['type'] == 'range':
                if partition['field'] not in fields:
                    schema.append(bigquery.SchemaField(partition['field'], 'INTEGER', mode='REQUIRED'))
            for field in clustering_fields:
                splits = field.split(':')
                name = splits[0]
                typ = splits[1]

                if name not in fields:
                    # When creating a table, automcatically add `ingestion_timestamp` to the schema to
                    # allow for checkpointing. Stu. M 1/20/20
                    # But only if it doesn't already exist in the schema
                    schema.append(bigquery.SchemaField(name, typ, mode="REQUIRED"))

        tbb = self.get_table_ref(name=tbl.table_id,
                                 project=project,
                                 schema=schema,
                                 dataset=dataset,
                                 partition=partition,
                                 clustering_fields=clustering_fields)

        self.client.create_table(tbb)

        return tbl

    def delete_dataset(self, dataset_name):
        dataset = self.dataset(dataset_name)

        def delete_tbls():
            tbls = self.client.list_tables(dataset)
            try:
                for item in tbls:
                    try:
                        self.client.delete_table(item)
                    except NotFound:
                        delete_tbls()
            except NotFound:
                delete_tbls()

        delete_tbls()
        self.client.delete_dataset(dataset)

    def delete_table(self, table_id):
        self.client.delete_table(table_id, not_found_ok=True)

    def get_table_ref(self, name, project, dataset, partition=None, schema=None, clustering_fields=None):
        tbb = bigquery.Table("{}.{}.{}".format(project, dataset.dataset_id, name),
                             schema=schema)
        if clustering_fields is not None and schema is not None:
            clustering_fields = list(map(lambda x: x.split(':')[0], clustering_fields))
            tbb.clustering_fields = clustering_fields
        if partition is not None:
            if partition['type'] == 'time':
                tbb.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    require_partition_filter=False
                )
            elif partition['type'] == 'range':
                tbb.range_partitioning = bigquery.RangePartitioning(bigquery.PartitionRange(partition['start'],
                                                                                            partition['end'],
                                                                                            partition['interval']),
                                                                    partition['field'])
        return tbb
