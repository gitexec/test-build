from google.cloud import bigquery
from migrations.migration import BigQueryMigration
import logging

dataset_name = 'lake'
table_name = 'pyr_resources'
# new_fields = {{FIELD_NAME}: {FIELD_TYPE}} wbrito 05/14/2020
new_fields = {'favorites_count': 'INTEGER', 'number_of_quiz_questions': 'INTEGER', 'file_s3_url': 'STRING'}


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    for f in table.schema:
        table_field = f.name
        if table_field in new_fields:
            logging.warning(f'{table_field} already added to {table_name} in {dataset_name} dataset!')
            new_fields.pop(table_field)
    for k, v in new_fields.items():
        new_schema.append(bigquery.SchemaField(k, v))

    if table.schema != new_schema:
        table.schema = new_schema
        migration.client.update_table(table, ['schema'])

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name.format(client.env))
    table = migration.client.get_table(dataset.table(table_name))
    for f in table.schema:
        table_field = f.name
        if table_field in new_fields:
            logging.warning(f'{table_field} already added to {table_name} in {dataset_name} dataset!')
    return dataset
