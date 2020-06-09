import click
from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud
from pathlib import PosixPath
import subprocess
import json
import os

dags_location = 'us-west3'
variables_file = 'template_variables.json'


@click.group()
@pass_environment
def cli(ctx):
    "Deploy dag to Google Cloud Composer"
    pass


@cli.command('deploy-dag', short_help='Deploy DAG to Cloud Composer')
@click.pass_context
@click.argument('env')
@click.argument('filename')
@click.argument('location')
@click.option('--requirements', '-r', is_flag=True, help="""Specify if any dependencies need to be install in
    the environment use y to confirm""")
@click.option('--helper_files', '-h', is_flag=True, help="""Copy the contents of dags/templates folder,
    libs folder, and plugins folder into cloudstorage used in supporting the dags.
    All files here must remain independant of all other files in our repo.""")
@click.option('--composer_vars', '-c', is_flag=True, help="""Set Cloud Composer with the variables in the
    composer_variables.json file.""")
@click.option('--airflow_vars', '-a', is_flag=True, help="""Set Airflow enviroment with the airflow variables in the
    variables.json file.""")
def deploy_dag(ctx, env, filename, location, requirements, helper_files, composer_vars, airflow_vars):
    """
    jeeves composer deploy-dag ENV TEMPLATE_FILENAME

    Deploy non-templated dag from dags directory.
    """
    _deploy_dag(ctx, env, filename, location, requirements, helper_files, composer_vars, airflow_vars)


@cli.command()
@click.argument('env')
@click.option('--requirements', '-r', is_flag=True, help="""Specify if any dependencies need to be install in
    the environment use y to confirm""", default=True)
@click.option('--helper_files', '-h', is_flag=True, help="""Copy the contents of dags/templates folder,
    libs folder, and plugins folder into cloudstorage used in supporting the dags.
    All files here must remain independant of all other files in our repo.""", default=True)
@click.option('--composer_vars', '-c', is_flag=True, help="""Set Cloud Composer with the variables in the
    composer_variables.json file.""", default=True)
@click.option('--airflow_vars', '-a', is_flag=True, help="""Set Airflow enviroment with the airflow variables in the
    variables.json file.""", default=True)
@click.argument('runtime_args', nargs=-1, required=False)
@pass_environment
def deploy_all(ctx, env, requirements, helper_files, composer_vars, airflow_vars, runtime_args):
    curr_path = os.path.dirname(__file__)
    vars_file_path = f'{curr_path}/{variables_file}'
    template_vars = _beam_vars(vars_file_path)
    dags = template_vars['dags']
    dag_templates = template_vars['dag_templates']
    clients = template_vars['clients']

    # deploy the dags file
    for d in dags:
        _deploy_dag(ctx, env, d, dags_location, requirements, helper_files, composer_vars, airflow_vars)

    for c in clients:
        for t in dag_templates:
            # Include any hard coded custom arguments
            template_args = (f'client={c}', f'env={env}')
            args = (template_args + runtime_args)
            _deploy_template(ctx, env, t, dags_location, requirements, helper_files, composer_vars, airflow_vars, args)


def _deploy_dag(ctx, env, filename, location, requirements, helper_files, composer_vars, airflow_vars):
    """
    jeeves composer deploy-dag ENV TEMPLATE_FILENAME

    Deploy non-templated dag from dags directory.
    """

    proj = gcloud.project(env)
    requirements_file = 'requirements.txt'
    airflow_variables_file = 'variables.json'
    composer_variables_file = 'composer_variables.json'
    dags_path = _dags_path().expanduser()
    airflow_path = _airflow_home_path().expanduser()
    commands = []

    filename = f'{filename}.py' if '.py' not in filename else filename
    imports = [{'group': 'dags', 'source': f'{dags_path}/{filename}'}]

    if helper_files:
        imports.extend([
            {'group': 'dags', 'source': f'{dags_path}/libs'},
            {'group': 'dags', 'source': f'{dags_path}/templates'},
            {'group': 'dags', 'source': f'{dags_path}/table_lists'},
            {'group': 'plugins', 'source': f'{airflow_path}/plugins/operators.py'},
            {'group': 'plugins', 'source': f'{airflow_path}/plugins/sensors.py'}])

    for i in imports:
        commands.append(
            ['gcloud',
             'composer',
             'environments',
             'storage',
             i['group'],
             'import',
             '--source', i['source'],
             '--environment', proj,
             '--location', location])

    if composer_vars:
        composer_variables = get_composer_variables(env,  _dags_path().expanduser() / composer_variables_file)
        commands.insert(0,
                        ['gcloud',
                         'composer',
                         'environments',
                         'update', proj,
                         '--update-env-variables', composer_variables,
                         '--location', location])

    if requirements:
        commands.insert(0,
                        ['gcloud',
                         'composer',
                         'environments',
                         'update', proj,
                         '--update-pypi-packages-from-file', requirements_file,
                         '--location', location])

    if airflow_vars:
        commands.append(['gcloud',
                         'composer',
                         'environments',
                         'storage',
                         'dags',
                         'import',
                         '--source', '{}/{}'.format(_airflow_home_path(), airflow_variables_file),
                         '--environment', proj,
                         '--location', location])

        commands.append(['gcloud',
                         'composer',
                         'environments',
                         'run', proj,
                         '--location', location,
                         'variables',
                         '--',
                         '-i',
                         f'/home/airflow/gcs/dags/{airflow_variables_file}'])

    for cmd in commands:
        click.echo('executing command:')
        click.echo(' '.join(cmd))
        process = subprocess.Popen(cmd,
                                   cwd=_dags_path(),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        for line in iter(process.stdout.readline, b''):
            click.echo(line)
        error_code = process.returncode
        if error_code is not None:
            click.echo(f'error: {error_code}')
            break


@cli.command('deploy-template', short_help='Deploy DAG template to Cloud Composer')
@click.pass_context
@click.argument('env')
@click.argument('filename')
@click.argument('location')
@click.option('--requirements', '-r', is_flag=True, help="""Specify if any dependencies need to be install in
    the environment use y to confirm""")
@click.option('--helper_files', '-h', is_flag=True, help="""Copy the contents of dags/templates folder,
    libs folder, and plugins folder into cloudstorage used in supporting the dags.
    All files here must remain independant of all other files in our repo.""")
@click.option('--composer_vars', '-c', is_flag=True, help="""Set Cloud Composer with the variables in the
    composer_variables.json file.""")
@click.option('--airflow_vars', '-a', is_flag=True, help="""Set Airflow enviroment with the airflow variables in the
    variables.json file.""")
@click.argument('template_vars', nargs=-1, required=True)
def deploy_template(ctx, env, filename, location, requirements, helper_files, composer_vars, airflow_vars, template_vars):
    _deploy_template(ctx, env, filename, location, requirements, helper_files, composer_vars, airflow_vars, template_vars)


def _deploy_template(ctx, env, filename, location, requirements, helper_files, composer_vars, airflow_vars, template_vars):
    """
    jeeves composer deploy-template ENV TEMPLATE_FILENAME TEMPLATE_VARS
    Deploy templated dag from templates directory.
    TEMPLATE_VARS Ex. client=bluesun foo=bar naughty=nice
    If ENV is 'local' then dag is built into dags directory but not pushed to composer.
    """
    placeholder_args = {}
    for item in template_vars:
        placeholder_args.update([item.split('=')])
    dag_filename = _build_dag_file(_dags_path(), filename, placeholder_args)
    if env == 'local':
        return dag_filename
    _deploy_dag(ctx, env, dag_filename, location, requirements, helper_files, composer_vars, airflow_vars)
    with _dags_path().expanduser() / dag_filename as f:
        f.unlink()


def _build_dag_file(temp_dir, template_filename, placeholder_args):
    try:
        template = _templates_path().expanduser() / '{}.py'.format(template_filename)
        template = template.read_text()
        start = template.index('$dag_filename$:') + len('$dag_filename$:')
        end = template.index('.py')
        filename = template[start: end].strip() + '.py'

        for key, value in placeholder_args.items():
            template = template.replace('--{}--'.format(key), value)
            filename = filename.replace('--{}--'.format(key), value)
        new_dag = _dags_path().expanduser() / filename
        new_dag.write_text(template)
        return filename
    except ValueError as err:
        raise Exception(f'{err}\nPlease include a filename in the template as a docstring.\
        \nEX: $dag_filename$: my_filename.py')


def _dags_path():
    path = PosixPath('/workspace/airflow/dags/')
    return path


def _templates_path():
    path = PosixPath('/workspace/airflow/templates/')
    return path


def _airflow_home_path():
    path = PosixPath('/workspace/airflow/')
    return path


def get_composer_variables(env, composer_variables_file):
    with open(str(composer_variables_file)) as data_file:
        file_content = json.load(data_file)
        default_vars = file_content['default']
        env_vars = file_content[env]
        default_vars.update(env_vars)
        vars = list(default_vars.items())
        vars = ','.join([f'{n[0]}={n[1]}' for n in vars])
        return vars


def _beam_vars(vars_file):
    with open(vars_file) as d:
        vars = json.load(d)
        return vars
