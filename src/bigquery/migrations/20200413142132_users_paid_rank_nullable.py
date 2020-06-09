import logging
from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'staging'
table_name = 'users'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))

    new_schema = []
    for f in table.schema:
        if f.name == 'client_paid_rank' or f.name == 'paid_rank':
            if f.mode == 'NULLABLE':
                logging.warning('paid_rank fields are already set to NULLABLE. Cannot do it again')
                return dataset

            new_schema.append(bigquery.SchemaField(f.name, f.field_type, 'NULLABLE'))
        else:
            new_schema.append(bigquery.SchemaField(f.name, f.field_type, mode=f.mode))

    table.schema = new_schema

    migration.client.update_table(table, ['schema'])

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))

    for f in table.schema:
        if f.name == 'client_paid_rank' or f.name == 'paid_rank':
            if f.mode == 'NULLABLE':
                logging.warning('paid_rank fields are already set to NULLABLE. Cannot do it again')

    return dataset
