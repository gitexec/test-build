from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'staging'
table_name = 'flat_site_visitors'


def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    flat_site_visitors = [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('site_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('tree_user_id', 'INTEGER'),
        bigquery.SchemaField('visitor_id', 'STRING'),
        bigquery.SchemaField('last_visit_date', 'DATETIME'),
        bigquery.SchemaField('visit_count', 'INTEGER'),
        bigquery.SchemaField('ipaddress', 'STRING'),
        bigquery.SchemaField('browser_agent', 'STRING'),
        bigquery.SchemaField('created_at', 'DATETIME'),
        bigquery.SchemaField('site_template_id', 'INTEGER'),
        bigquery.SchemaField('active', 'INTEGER'),
        bigquery.SchemaField('third_party_tracking_company', 'STRING'),
        bigquery.SchemaField('tracking_code', 'STRING'),
        bigquery.SchemaField('owner_name', 'STRING'),
        bigquery.SchemaField('email', 'STRING'),
        bigquery.SchemaField('story', 'STRING'),
        bigquery.SchemaField('avatar_file_name', 'STRING')
    ]

    client.create_table(name=table_name,
                        project=client.client.project,
                        schema=flat_site_visitors,
                        dataset=dataset,
                        partition={'type': 'range',
                                   'field': 'client_partition_id',
                                   'start': 1,
                                   'end': 100,
                                   'interval': 1},
                        clustering_fields=['leo_eid:STRING', 'ingestion_timestamp:TIMESTAMP'])
    return dataset


def down(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)
    table = client.client.get_table(dataset.table(table_name))
    client.delete_table(table)
