from google.cloud import bigquery
from migrations.migration import BigQueryMigration


def up(client):
    client = BigQueryMigration(client)
    dataset = client.create_dataset('pii')

    pii_schema = [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("icentris_client", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tree_user_id", "INTEGER"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("mobile_phone", "STRING"),
        bigquery.SchemaField("street", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("birth_date", "DATE"),
        bigquery.SchemaField("gender", "STRING")
    ]

    client.create_table(name='users',
                        project=client.client.project,
                        schema=pii_schema, dataset=dataset,
                        partition={'type': 'range',
                                   'field': 'client_partition_id',
                                   'start': 1,
                                   'end': 100,
                                   'interval': 1},
                        clustering_fields=['leo_eid:STRING', 'ingestion_timestamp:TIMESTAMP'])
    return dataset


def down(client):
    migration = BigQueryMigration(client)
    migration.delete_dataset('pii')
