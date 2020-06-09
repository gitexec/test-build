"""
cli utility. create migrations, deploy with up or rollback with down
"""
import click
import datetime
from google.cloud import bigquery
from pathlib import PosixPath
from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud
from jeeves.commands.libs import import_file
from pprint import pprint


@click.group()
@pass_environment
def cli(ctx):
    pass


def bigquery_client(env):
    client = bigquery.Client(project=gcloud.project(env))
    client.env = env
    return client


def get_migration_files_path():
    path = PosixPath('/workspace/bigquery/migrations')
    path.expanduser()
    return path


def get_migration_files():
    return sorted(get_migration_files_path().expanduser().glob('*.py'))


def get_bigquery_migrations(client):
    already_run = {}
    batch_num = 0
    job = client.query('SELECT * FROM system.migrations')
    rs = job.result()
    for r in rs:
        already_run[r['migration']] = r
        if r['batch_num'] > batch_num:
            batch_num = r['batch_num']
    return (already_run, batch_num)


def initialize_system(client):
    system_dataset = bigquery.Dataset('{}.{}'.format(client.project, 'system'))
    system_dataset.location = 'us-west3'
    migrations_tbl = system_dataset.table('migrations')
    migrations_schema = [
        bigquery.SchemaField("dataset", "STRING"),
        bigquery.SchemaField("migration", "STRING"),
        bigquery.SchemaField("batch_num", "INTEGER"),
        bigquery.SchemaField('created', 'TIMESTAMP')
    ]

    migrations_tbl = bigquery.Table(migrations_tbl, schema=migrations_schema)

    if not any('system' in ds.dataset_id for ds in client.list_datasets()):
        client.create_dataset(system_dataset)
        client.create_table(migrations_tbl)

    return (system_dataset, migrations_tbl)


@cli.command()
@click.argument('name')
@pass_environment
def create(ctx, name):
    for m in get_migration_files():
        m = str(m)
        if name in m:
            raise Exception('migration already exists!')

    template = get_migration_files_path().expanduser() / 'template'
    template = template.read_text()
    ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename = '{}_{}.py'.format(ts, name)
    new_migration = get_migration_files_path().expanduser() / filename
    new_migration.write_text(template)


@cli.command()
@click.argument('env')
@pass_environment
def up(ctx, env):
    client = bigquery_client(env)
    system, migrations_tbl = initialize_system(client)
    bq_migrations, latest_batch_num = get_bigquery_migrations(client)

    to_run = get_migration_files()
    import_file(str(get_migration_files_path().expanduser() / 'migration.py'))
    successful_runs = []
    batch_num = latest_batch_num + 1
    current = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    for f in to_run:
        f = str(f)
        if f not in bq_migrations and '__init__.py' not in f and 'migration.py' not in f:
            migration = import_file(f)
            try:
                print(f)
                dataset = migration.up(client=client)
                run = (dataset.dataset_id, f, batch_num, current)
                """
                @author S.Murry 
                @date 11/6/19
                Can't use streaming (`insert_rows`) interface here,
                because there is at least a two hour delay before it can be deleted.
                Need to use the `query` interface instead, so we can rollback immediately if needed.
                https://stackoverflow.com/q/43085896/20178
                https://cloud.google.com/bigquery/streaming-data-into-bigquery
                """
                dml = "INSERT INTO `{}.{}.{}` (dataset, migration, batch_num, created) \
                       values ('{}', '{}', {}, '{}')".format(
                    migrations_tbl.project,
                    migrations_tbl.dataset_id,
                    migrations_tbl.table_id,
                    run[0],
                    run[1],
                    run[2],
                    run[3])
                query_job = client.query(dml)
                query_job.result()
                successful_runs.append(run)
            except Exception as e:
                pprint(e)
                # S. Murry 11/14/19 - Cannot use @pass_context because it collides with other annotations
                # causing duplicate values to passed into the arguments.  For Example: 2 `env` will be passed,
                # which causes python to throw an exception.
                if (len(successful_runs) > 0):
                    cctx = click.get_current_context()
                    cctx.invoke(down, env=env)
                break


@cli.command()
@click.option('--batches', default=1, help='number of batches to rollback')
@click.argument('env')
@pass_environment
def down(ctx, env, batches):
    print("Rolling back: {}".format(env))
    client = bigquery_client(env)
    system, migrations_tbl = initialize_system(client)

    dml = "SELECT batch_num, ARRAY_AGG(migration ORDER BY migration DESC) as migrations\
        FROM `{}.system.migrations`\
        GROUP BY batch_num\
        ORDER BY batch_num DESC \
        LIMIT {}".format(migrations_tbl.project, batches)

    query_job = client.query(dml)
    results = query_job.result()
    import_file(str(get_migration_files_path().expanduser() / 'migration.py'))
    if results.total_rows > 0:
        for r in results:
            for f in r.migrations:
                print(f)
                migration = import_file(f)
                migration.down(client=client)
                dml = "DELETE FROM `{}.{}.{}` WHERE migration='{}'".format(
                    migrations_tbl.project,
                    migrations_tbl.dataset_id,
                    migrations_tbl.table_id,
                    f)
                query_job = client.query(dml)
                results = query_job.result()
    else:
        print("No migrations available.")
