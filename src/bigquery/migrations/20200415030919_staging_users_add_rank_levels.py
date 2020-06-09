import logging
from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'staging'
table_name = 'users'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    if table.schema[-1].name == 'client_paid_rank_level':
        logging.warning(f'rank levels  already added to {table_name} in {dataset_name} dataset!')
        return dataset

    new_schema.extend([
        bigquery.SchemaField('lifetime_rank_level', 'INTEGER'),
        bigquery.SchemaField('paid_rank_level', 'INTEGER'),
        bigquery.SchemaField('client_lifetime_rank_level', 'INTEGER'),
        bigquery.SchemaField('client_paid_rank_level', 'INTEGER')
    ])
    table.schema = new_schema

    migration.client.update_table(table, ['schema'])

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))

    if table.schema[-1].name == 'client_paid_rank_level':
        logging.warning(f'Cannot remove rank_level columns from table {table_name}')

    return dataset
