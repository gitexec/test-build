from google.cloud import bigquery
from migrations.migration import BigQueryMigration


dataset_name = 'staging'
table_name = 'product_reviews'


def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    product_reviews = [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('site_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('tree_user_id', 'INTEGER'),
        bigquery.SchemaField('product_code', 'STRING'),
        bigquery.SchemaField('product_name', 'STRING'),
        bigquery.SchemaField('product_description', 'STRING'),
        bigquery.SchemaField('product_short_description', 'STRING'),
        bigquery.SchemaField('product_available_on', 'DATETIME'),
        bigquery.SchemaField('product_discontinued_on', 'DATETIME'),
        bigquery.SchemaField('product_slug', 'STRING'),
        bigquery.SchemaField('product_avg_rating', 'NUMERIC'),
        bigquery.SchemaField('product_reviews_count', 'INTEGER'),
        bigquery.SchemaField('review_name', 'STRING'),
        bigquery.SchemaField('location', 'STRING'),
        bigquery.SchemaField('rating', 'INTEGER'),
        bigquery.SchemaField('title', 'STRING'),
        bigquery.SchemaField('review', 'STRING'),
        bigquery.SchemaField('approved', 'BOOLEAN'),
        bigquery.SchemaField('created_at', 'DATETIME'),
        bigquery.SchemaField('updated_at', 'DATETIME'),
        bigquery.SchemaField('ip_address', 'STRING'),
        bigquery.SchemaField('show_identifier', 'BOOLEAN'),
    ]

    client.create_table(name=table_name,
                        project=client.client.project,
                        schema=product_reviews,
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
