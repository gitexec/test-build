from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'staging'
table_name = 'orders'


def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    table = client.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    new_schema.insert(1, bigquery.SchemaField('commission_user_id', 'INTEGER', mode='REQUIRED'))

    client.delete_table(table)
    client.create_table(name=table_name,
                        project=client.client.project,
                        schema=new_schema,
                        dataset=dataset,
                        clustering_fields=['leo_eid:STRING', 'ingestion_timestamp:TIMESTAMP'],
                        partition={'type': 'range',
                                   'field': 'client_partition_id',
                                   'start': 1,
                                   'end': 100,
                                   'interval': 1})
    return dataset


def down(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    table = client.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    if new_schema[1].name == 'commission_user_id':
        new_schema.pop(1)
        client.delete_table(table)
        client.create_table(name=table_name,
                            project=client.client.project,
                            schema=new_schema,
                            dataset=dataset,
                            partition={'type': 'range',
                                       'field': 'client_partition_id',
                                       'start': 1,
                                       'end': 100,
                                       'interval': 1},
                            clustering_fields=['leo_eid:STRING', 'ingestion_timestamp:TIMESTAMP'])

    return dataset
