import logging
from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'lake'
table_name = 'tree_order_items'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    if table.schema[-1].name == 'product_type':
        logging.warning(f'product_type already added to {table_name} in {dataset_name} dataset!')
        return dataset

    new_schema.append(bigquery.SchemaField('product_type', 'STRING'))
    table.schema = new_schema

    migration.client.update_table(table, ['schema'])

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))

    if table.schema[-1].name == 'product_type':
        logging.warning(f'Cannot remove column product_type from table {table_name}')

    return dataset
