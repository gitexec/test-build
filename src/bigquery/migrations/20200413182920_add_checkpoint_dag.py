from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'system'
table_name = 'checkpoint'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    del new_schema[1]
    new_schema.insert(0, bigquery.SchemaField('dag_id', 'STRING', mode='REQUIRED'))

    migration.delete_table(table)
    migration.create_table(
        name=table_name,
        project=migration.client.project,
        schema=new_schema,
        dataset=dataset)
    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    table = migration.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    if new_schema[0].name == 'dag_id':
        new_schema.pop(0)

        new_schema.insert(1, bigquery.SchemaField('leo_eid', 'STRING', mode="NULLABLE"))

        migration.delete_table(table)
        migration.create_table(
            name=table_name,
            project=migration.client.project,
            schema=new_schema,
            dataset=dataset)

    return dataset
