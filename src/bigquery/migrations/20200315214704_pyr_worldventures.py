from migrations.migration import BigQueryMigration

dataset_name = 'pyr_worldventures_{}'


def up(client):
    migration = BigQueryMigration(client)

    global dataset_name
    dataset_name = dataset_name.format(client.env)
    wv_ds = migration.create_dataset(dataset_name)
    bs_ds = migration.dataset('pyr_bluesun_{}'.format(client.env))

    ls = client.list_tables(bs_ds)
    for tbl in ls:
        tbl_ref = bs_ds.table(tbl.table_id)
        tbl = client.get_table(tbl_ref)
        migration.create_table(tbl_ref.table_id, tbl_ref.project, wv_ds, schema=tbl.schema)

    return wv_ds


def down(client):
    migration = BigQueryMigration(client)

    #  required code for deleting a newly created dataset -- ndg 2/5/20
    migration.delete_dataset(dataset_name.format(client.env))
