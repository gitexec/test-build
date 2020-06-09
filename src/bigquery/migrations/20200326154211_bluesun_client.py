from migrations.migration import BigQueryMigration


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset('system')

    sql = f"""
        INSERT INTO {migration.client.project}.system.clients (icentris_client, partition_id, wrench_id) VALUES
        ('bluesun', 4, '5396d067-9e31-4572-951a-a7d1b0a5eaf6')"""
    job = migration.client.query(sql)
    job_result = job.result()

    print(job_result)

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset('system')

    migration.client.query(f'DELETE FROM {migration.client.project}.system.clients WHERE icentris_client = "bluesun"')

    return dataset
