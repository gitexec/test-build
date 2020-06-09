import click
from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud
from pathlib import PosixPath
import subprocess
import json
import os

dataflow_directory = PosixPath('/workspace/dataflow/')
variables_file = 'template_variables.json'
df_location = 'us-west3'


@click.group()
@pass_environment
def cli(ctx):
    pass


@cli.command()
@click.argument('env')
@click.argument('region')
@click.argument('template')
@click.argument('deploytime_args', nargs=-1, required=True)
@pass_environment
def deploy_template(ctx, env, region, template, deploytime_args):
    """
    Full Command to Deploy Template to DataFlow:
    python load_sql_to_bq.py \
        --project icentris-ml \
        --staging_location gs://icentris-ml-dataflow/staging \
        --temp_location gs://icentris-ml-dataflow/tmp \
        --template_location gs://icentris-ml-dataflow/templates/load_sql_to_bq \
        --runner DataflowRunner \
        --region us-west3
        --setup_file ./setup.py
    """
    _deploy_template(ctx, env, region, template, deploytime_args)


@cli.command()
@click.argument('env')
@click.argument('runtime_args', nargs=-1)
@pass_environment
def deploy_all(ctx, env, runtime_args):
    curr_path = os.path.dirname(__file__)
    vars_file_path = f'{curr_path}/{variables_file}'
    job_vars = _beam_vars(vars_file_path)
    templates = job_vars['beam_templates']
    clients = job_vars['clients']
    for c in clients:
        for t in templates:
            # Include any hard coded custom arguments
            template_args = (f'client={c}', f'env={env}')
            args = (template_args + runtime_args)
            _deploy_template(ctx, env, df_location, t, args)


def _deploy_template(ctx, env, region, template, deploytime_args):
    proj = gcloud.project(env)
    cmd = ['python',
           template+'.py',
           '--project', proj,
           '--staging_location', 'gs://{}-dataflow/staging'.format(proj),
           '--temp_location', 'gs://{}-dataflow/tmp'.format(proj),
           '--template_location', 'gs://{}-dataflow/templates/{}'.format(proj, template),
           '--runner', 'DataflowRunner',
           '--region', region,
           '--setup_file', './setup.py']
    for a in deploytime_args:
        arg, val = a.split('=')
        cmd.append(f'--{arg}')
        cmd.append(val)
    click.echo('deploying template with command:')
    click.echo(' '.join(cmd))
    process = subprocess.Popen(cmd,
                               cwd=dataflow_directory,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    for line in iter(process.stdout.readline, b''):
        click.echo(line)


def _beam_vars(vars_file):
    with open(vars_file) as d:
        vars = json.load(d)
        return vars
