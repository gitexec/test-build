import click
import logging
import subprocess
import sys

from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud

log = logging.getLogger()
log.setLevel('DEBUG')


@click.group()
@pass_environment
def cli(ctx):
    pass


@cli.command()
@click.argument('env')
@click.option('--secret', type=click.Tuple([str, str]), multiple=True)
@click.option('--sql-password')
@click.option('--silent', '-y', is_flag=True)
@pass_environment
def create(ctx, env, secret, sql_password, silent):

    orig_project = gcloud.config()['project']

    project = gcloud.project(env)

    def _finish():
        print('resetting original project')
        subprocess.run('gcloud config set project {}'.format(orig_project).split(' '), check=True)
        print('finished')

    def _run(cmd):
        print('running command:')
        print(' '.join(cmd.split()))
        if not silent:
            confirmation = input('Continue? y/n/c\n')
            if confirmation.lower() in ['c', 'cancel']:
                print('terminating!')
                _finish()
                sys.exit()
            elif confirmation.lower() in ['n', 'no']:
                print('skipping!')
                return

        rs = subprocess.run(' '.join(cmd.split()),
                            check=True,
                            shell=True,
                            universal_newlines=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

        if rs.returncode != 0:
            raise Exception(rs.stderr)

    subprocess.run('gcloud config set project {}'.format(project).split(' '), check=True)

    rs = subprocess.run('gcloud config list'.split(' '),
                        universal_newlines=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
    print('config setup is:')
    print(rs.stdout)

    cmds = [
        # create composer environment
        """
    gcloud composer environments create {}
        --async
        --location us-central1
        --image-version composer-1.9.2-airflow-1.10.6
        --disk-size 100
        --machine-type n1-standard
        --zone us-central1-f
        --node-count 3
        --python-version 3
    """.format(project),
        # enable kms api
        """
    gcloud services enable cloudkms.googleapis.com
    """,
        # create configs keyring
        """
    gcloud kms keyrings create configs --location us-west3
    """,
        # create configs key
        """
     gcloud kms keys create kek-{}1
        --keyring configs
        --purpose encryption
        --location us-west3
     """.format(env[0])
    ]

    for s in secret:
        secret_cmd = """
            jeeves crypto create-secret {env} us-west3 configs kek-{lane}1 {name} {value}
            """.format(env=env, lane=env[0], name=s[0], value=s[1])
        cmds.append(secret_cmd)

    if env != 'local':
        sql_instance = """
            gcloud sql instances create icentris-vibe-dbs
                --async
                --region us-west3
                --database-version MYSQL_5_7
                --storage-type {}
                --tier {}
                --root-password "{}"
        """

        if env == 'prd':
            sql_instance = sql_instance.format('SSD', 'db-n1-standard-8', sql_password)
        else:
            sql_instance = sql_instance.format('HDD', 'db-f1-micro', sql_password)

        cmds.append(sql_instance)

    buckets = [
        ('airflow', '', ''),
        ('dataflow', '', ''),
        ('wrench-exports', '', ''),
        ('wrench-imports', '', ''),
        ('vibe-schemas', '', ''),
        ('cdc-imports', '', ''),
        ('cdc-imports-processed', '', True),
    ]

    for name, retention, lifecycle in buckets:
        if retention:
            retention = '--retention {}'.format(retention)
        cmds.append("""
            gsutil mb -l us-west3 {retention} gs://{project}-{name}
        """.format(retention=retention, project=project, name=name))
        if lifecycle:
            cmds.append("""
                gsutil lifecycle set /workspace/jeeves/jeeves/commands/lifecycle.json gs://{project}-{name}
            """.format(lifecycle=lifecycle, project=project, name=name))


    # Create custom roles
    create_dataflow_role = """
    gcloud iam roles create icentris.dataflow
        --project {project}
        --title 'iCentris Dataflow Role'
        --description 'Used by custom Dataflow service account'
        --stage GA
        --permissions bigquery.jobs.create,cloudkms.cryptoKeyVersions.useToDecrypt,\
cloudsql.instances.connect,cloudsql.instances.get,dataflow.jobs.get,\
logging.logEntries.create,monitoring.metricDescriptors.list,\
secretmanager.versions.access
    """.format(project=project)
    cmds.append(create_dataflow_role)

    create_databus_role = """
    gcloud iam roles create icentris.databus
        --project {project}
        --title 'iCentris Databus Role'
        --description 'Used by the leo-bus to interact with GCP'
        --stage GA
        --permissions storage.buckets.list,storage.objects.create,\
storage.objects.delete
    """.format(project=project)
    cmds.append(create_databus_role)

    create_cicd_role = """
    gcloud iam roles create icentris.cicd
        --project {project}
        --title 'iCentris CICD Role'
        --description 'Run CICD cloud build'
        --stage GA
        --permissions bigquery.datasets.create,\
bigquery.datasets.get,bigquery.jobs.create,\
bigquery.tables.create,bigquery.tables.get,\
bigquery.tables.getData,bigquery.tables.list,\
bigquery.tables.updateData,\
cloudkms.cryptoKeyVersions.useToDecrypt,\
cloudsql.instances.connect,cloudsql.instances.get,\
dataflow.jobs.get,logging.logEntries.create,\
monitoring.metricDescriptors.list,\
resourcemanager.projects.get,\
secretmanager.versions.access,storage.buckets.list
    """.format(project=project)
    cmds.append(create_cicd_role)

    # Create service accounts
    cmds.append('gcloud iam service-accounts create icentris-databus')
    cmds.append('gcloud iam service-accounts create icentris-dataflow')
    cmds.append('gcloud iam service-accounts create icentris-cicd')

    # Apply roles to service accounts
    add_databus_role = """
    gcloud projects add-iam-policy-binding {project} --member \
    serviceAccount:icentris-databus@{project}.iam.gserviceaccount.com --role projects/{project}/roles/icentris.databus
    """.format(project=project)
    cmds.append(add_databus_role)

    add_dataflow_worker_role = """
    gcloud projects add-iam-policy-binding {project} --member \
    serviceAccount:icentris-dataflow@{project}.iam.gserviceaccount.com --role roles/dataflow.worker
    """.format(project=project)
    cmds.append(add_dataflow_worker_role)

    add_service_agent_role = """
    gcloud projects add-iam-policy-binding {project} --member \
    serviceAccount:icentris-dataflow@{project}.iam.gserviceaccount.com --role roles/dataflow.serviceAgent
    """.format(project=project)
    cmds.append(add_service_agent_role)

    add_dataflow_role = """
    gcloud projects add-iam-policy-binding {project} --member \
    serviceAccount:icentris-dataflow@{project}.iam.gserviceaccount.com \
    --role projects/{project}/roles/icentris.dataflow
    """.format(project=project)
    cmds.append(add_dataflow_role)

    add_cicd_role = """
    gcloud projects add-iam-policy-binding {project} --member \
    serviceAccount:icentris-cicd@{project}.iam.gserviceaccount.com --role projects/{project}/roles/icentris.cicd
    """.format(project=project)
    cmds.append(add_cicd_role)

    for cmd in cmds:
        _run(cmd)

    _finish()
