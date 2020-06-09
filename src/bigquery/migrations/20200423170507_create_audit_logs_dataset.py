from migrations.migration import BigQueryMigration


dataset_name = 'audit_logs'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.create_dataset(dataset_name)
    return dataset


def down(client):
    migration = BigQueryMigration(client)
    migration.delete_dataset(dataset_name)
