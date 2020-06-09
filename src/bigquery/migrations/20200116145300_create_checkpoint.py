from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'system'
table_name = 'checkpoint'


def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    # In order to use clusters in BigQuery, the must be inside partitions.
    # Even though TIMESTAMP is supported as a partition type,
    # a partition can only be done by date not datetime.  Furthermore, if timestamp is used
    checkpoint_schema = [
        bigquery.SchemaField("table", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("leo_eid", "STRING", mode="REQUIRED"),  # Partition
        bigquery.SchemaField("checkpoint", "TIMESTAMP", mode="REQUIRED"),  # Partition
    ]

    client.create_table(
        name=table_name,
        project=client.client.project,
        schema=checkpoint_schema,
        dataset=dataset)
    return dataset


def down(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)
    table = client.client.get_table(dataset.table(table_name))
    client.delete_table(table)
