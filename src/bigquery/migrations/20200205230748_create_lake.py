from migrations.migration import BigQueryMigration

dataset_name = 'lake'


def up(client):
    migration = BigQueryMigration(client)

    dataset = migration.create_dataset(dataset_name)

    parent_dataset = migration.dataset('pyr_bluesun_{}'.format(client.env))
    tbls = migration.client.list_tables(parent_dataset)

    clusters = migration.default_clustering_fields

    clusters.insert(0, 'icentris_client:STRING')

    for item in tbls:
        tbl = migration.client.get_table(item.reference)

        orig = tbl.schema

        new = orig.copy()

        migration.create_table(name=tbl.table_id,
                               project=migration.client.project, schema=new, dataset=dataset,
                               partition={'type': 'time'},
                               clustering_fields=clusters)

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    migration.delete_dataset(dataset_name)
