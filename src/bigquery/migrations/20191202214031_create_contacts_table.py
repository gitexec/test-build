from google.cloud import bigquery
from migrations.migration import BigQueryMigration


dataset_name = 'staging'
table_name = 'contacts'


def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    contacts = [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('owner_tree_user_id', 'INTEGER'),
        bigquery.SchemaField('owner_user_id', 'INTEGER'),
        bigquery.SchemaField('tree_user_id', 'INTEGER'),
        bigquery.SchemaField('user_id', 'INTEGER'),
        bigquery.SchemaField('type', 'INTEGER'),
        bigquery.SchemaField('level_of_interest', 'INTEGER'),
        bigquery.SchemaField('first_name', 'STRING'),
        bigquery.SchemaField('last_name', 'STRING'),
        bigquery.SchemaField('birthday', 'DATE'),
        bigquery.SchemaField('created_at', 'DATETIME'),
        bigquery.SchemaField('updated_at', 'DATETIME'),
        bigquery.SchemaField('is_downline', 'BOOLEAN'),
        bigquery.SchemaField('opt_in', 'BOOLEAN'),
        bigquery.SchemaField('info', 'STRING'),
        bigquery.SchemaField('avatar_file_name', 'STRING'),
        bigquery.SchemaField('avatar_content_type', 'STRING'),
        bigquery.SchemaField('avatar_file_size', 'INTEGER'),
        bigquery.SchemaField('avatar_updated_at', 'DATETIME'),
        bigquery.SchemaField(
            'addresses',
            'RECORD',
            mode='REPEATED',
            fields=[
                bigquery.SchemaField('address1', 'STRING'),
                bigquery.SchemaField('address2', 'STRING'),
                bigquery.SchemaField('city', 'STRING'),
                bigquery.SchemaField('state', 'STRING'),
                bigquery.SchemaField('postal_code', 'STRING'),
                bigquery.SchemaField('country', 'STRING'),
            ]
        ),
        bigquery.SchemaField(
            'emails',
            'RECORD',
            mode='REPEATED',
            fields=[
                bigquery.SchemaField('email', 'STRING'),
                bigquery.SchemaField('preferred', 'BOOLEAN'),
                bigquery.SchemaField('undeliverable_count', 'INTEGER'),
                bigquery.SchemaField('spam_reported_count', 'INTEGER'),
                bigquery.SchemaField('source', 'STRING'),
            ]
        ),
        bigquery.SchemaField(
            'phone_numbers',
            'RECORD',
            mode='REPEATED',
            fields=[
                bigquery.SchemaField('phone_number', 'STRING'),
                bigquery.SchemaField('label', 'STRING'),
                bigquery.SchemaField('unformatted_phone_number', 'STRING'),
                bigquery.SchemaField('source', 'STRING'),
                bigquery.SchemaField('dialing_code', 'STRING'),
            ]
        ),
        bigquery.SchemaField(
            'categories',
            'RECORD',
            mode='REPEATED',
            fields=[
                bigquery.SchemaField('category', 'STRING'),
                bigquery.SchemaField('created_at', 'DATETIME'),
                bigquery.SchemaField('updated_at', 'DATETIME'),
            ]
        ),
    ]

    client.create_table(name=table_name,
                        project=client.client.project,
                        schema=contacts,
                        partition={'type': 'range',
                                   'field': 'client_partition_id',
                                   'start': 1,
                                   'end': 100,
                                   'interval': 1},
                        dataset=dataset,
                        clustering_fields=['leo_eid:STRING', 'ingestion_timestamp:TIMESTAMP'])
    return dataset


def down(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)
    table = client.client.get_table(dataset.table(table_name))
    client.delete_table(table)
