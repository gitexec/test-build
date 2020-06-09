import json
from migrations.migration import BigQueryMigration
from pathlib import PosixPath
from google.cloud import bigquery

dataset_name = 'pyr_bluesun'


def up(client):
    migration = BigQueryMigration(client)
    name = dataset_name + '_{}'.format(client.env)

    with PosixPath('/workspace/bigquery/migrations/bluesun_schema.json').open(mode='r') as f:
        tbls = json.loads(f.read())

    dataset = migration.create_dataset(name)

    for tbl, raw in tbls.items():
        schema = []
        for f in raw['fields']:
            schema.append(bigquery.SchemaField(f['name'], f['type'], f['mode']))

        migration.create_table(name=tbl,
                               project=migration.client.project, schema=schema, dataset=dataset)

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    name = dataset_name + '_{}'.format(client.env)
    migration = BigQueryMigration(client)
    migration.delete_dataset(name)
