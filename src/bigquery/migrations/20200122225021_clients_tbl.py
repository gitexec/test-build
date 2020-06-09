from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'system'
table_name = 'clients'


def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    schema = [
        bigquery.SchemaField("icentris_client", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("partition_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("wrench_id", "STRING", mode="REQUIRED")
    ]

    tbl = client.create_table(
        name=table_name,
        project=client.client.project,
        schema=schema,
        dataset=dataset)

    client.client.insert_rows(client.client.get_table(tbl),
                              [('monat', 1, '2c889143-9169-436a-b610-48c8fe31bb87'),
                               ('worldventures', 2, 'd7d3e26f-d105-4816-825d-d5858b9cf0d1'),
                               ('naturessunshine', 3, '16bcfb48-153a-4c7d-bb65-19074d9edb17')])

    return dataset


def down(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)
    table = client.client.get_table(dataset.table(table_name))
    client.delete_table(table)
