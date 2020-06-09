from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'lake'
table_name = 'wrench_metrics'


def up(client):
    migration = BigQueryMigration(client)

    dataset = migration.dataset(dataset_name)  # use me if you are NOT creating a new dataset. -- ndg 2/5/20

    clusters = migration.default_clustering_fields

    del clusters[0]

    clusters.insert(0, 'client_wrench_id:STRING')

    schema = [
        bigquery.SchemaField('entity_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("tree_user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("prediction", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("client_wrench_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("expirement_name", "STRING"),
        bigquery.SchemaField("processing_datetime", "DATETIME")
    ]

    migration.create_table(name=table_name,
                           project=migration.client.project, schema=schema, dataset=dataset,
                           partition={'type': 'time'},
                           clustering_fields=clusters)

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)
    table = migration.client.get_table(dataset.table(table_name))
    migration.delete_table(table)
