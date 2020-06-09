import pytest

from pathlib import PosixPath

from google.cloud import bigquery
from google.cloud.exceptions import Conflict


@pytest.fixture
def setup(env, load_dag):
    dag_file = None
    template_name = None

    def _setup(name):
        nonlocal dag_file
        nonlocal template_name
        template_name = name
        dag_file = _build_dag(template_name, {'client': 'bluesun'})
        print(dag_file)
        _up_bluesun_dataset(template_name, env)
        return load_dag(dag_file)

    try:
        yield _setup
    except Exception as e:
        print(e)
        pass
    finally:
        dag_file.unlink()
        _down_bluesun_dataset(template_name, env)


def _build_dag(template_name, placeholder_args):
    try:
        template = _template_files_path() / '{}.py'.format(template_name)
        template = template.read_text()
        start = template.index('$dag_filename$:') + len('$dag_filename$:')
        end = template.index('.py')
        filename = template[start: end].strip() + '.py'

        for key, value in placeholder_args.items():
            template = template.replace('--{}--'.format(key), value)
            filename = filename.replace('--{}--'.format(key), value)
        new_dag = _dag_files_path() / filename
        new_dag.write_text(template)
    except ValueError as err:
        raise Exception(f'{err}\nPlease include a filename in the template as a docstring.\
        \nEX: $dag_filename$: my_filename.py')
    return new_dag


def _template_files_path():
    path = PosixPath('/workspace/airflow/templates/')
    path.expanduser()
    return path


def _dag_files_path():
    path = PosixPath('/workspace/airflow/dags/')
    path.expanduser()
    return path


def _up_bluesun_dataset(template_name, env):
    return
    dataset_name = 'pyr_bluesun_test_{}_{}'.format(template_name, env['user'])
    client = bigquery.Client(env['project'])

    dataset = client.dataset(dataset_name)

    try:
        client.create_dataset(dataset)
    except Conflict:
        _down_bluesun_dataset(template_name, env)
        _up_bluesun_dataset(template_name, env)

    tbl_ref = bigquery.Table("{}.{}.{}".format(env['project'], dataset.dataset_id, 'pyr_firefly_characters'),
                             schema=[
        bigquery.SchemaField('name', 'STRING', mode='REQUIRED')
    ])

    client.create_table(tbl_ref)

    client.insert_rows(tbl_ref,
                       [('Mal'),
                        ('Jane'),
                        ('Anora')])


def _down_bluesun_dataset(template_name, env):
    return
    dataset_name = 'pyr_bluesun_test_{}_{}'.format(template_name, env['user'])
    client = bigquery.Client(env['project'])

    dataset = client.dataset(dataset_name)
    tbl = dataset.table('pyr_firefly_characters')

    client.delete_table(tbl, not_found_ok=True)

    client.delete_dataset(dataset, not_found_ok=True)
