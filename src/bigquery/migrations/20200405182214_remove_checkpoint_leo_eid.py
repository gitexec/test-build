import logging
from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'system'
table_name = 'checkpoint'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))

    if table.schema[1].mode == 'NULLABLE':
        logging.warning('leo_eid is already set to NULLABLE. Cannot do it again')
        return dataset

    new_schema = []
    for f in table.schema:
        if f.name == 'leo_eid':
            new_schema.append(bigquery.SchemaField('leo_eid', 'STRING', mode='NULLABLE'))
        else:
            new_schema.append(bigquery.SchemaField(f.name, f.field_type, mode=f.mode))

    table.schema = new_schema

    migration.client.update_table(table, ['schema'])

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))

    if table.schema[1].mode == 'NULLABLE':
        logging.warning('leo_eid is already set to NULLABLE. Cannot roll this one back')

    return dataset
