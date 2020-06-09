import click
import logging
import warnings

from google.cloud import secretmanager_v1beta1 as secretmanager
from google.api_core.exceptions import NotFound
from pyconfighelper import ConfigHelper
from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud

warnings.simplefilter("ignore")

log = logging.getLogger()
log.setLevel('DEBUG')


@click.group()
@pass_environment
def cli(ctx):
    pass


@cli.command()
@pass_environment
def create_keyring(ctx, env, location, keyring):
    pass


@cli.command()
@pass_environment
def create_key(ctx, env, location, keyring, keyname, rotation):
    pass


@cli.command()
@click.argument('env')
@click.argument('location')
@click.argument('keyring')
@click.argument('keyname')
@click.argument('plaintext')
@click.pass_context
def encode_secret(ctx, env, location, keyring, keyname, plaintext):
    """
    Encode a secret with the appropriate key from gcloud crypto keys

    jeeves crypto encode-secret ENV LOCATION KEYRING KEYNAME PLAINTEXT

    env = local, dev, txt, prd
    location = us-west3, us-west2, etc

    """
    helper = ConfigHelper(
        kms_project=gcloud.project(env),
        kms_location=location,
        kms_key_ring=keyring,
        kms_key_name=keyname,
        log_level=logging.ERROR)

    secret = helper.encode_secret(plaintext)
    print(secret)
    return secret


@cli.command()
@click.argument('env')
@click.argument('location')
@click.argument('keyring')
@click.argument('keyname')
@click.argument('secret_name')
@click.argument('plaintext')
@click.pass_context
def create_secret(ctx, env, location, keyring, keyname, secret_name, plaintext):
    """
    jeeves crypt create-secret ENV LOCATION KEYRING KEYNAME SECRET_NAME PLAINTExT

    env = local, dev, tst, prd

    location = 'us-west3', 'us-west2', etc

    Encodes the plaintext secret and writes it to gcloud secrets
    """
    encoded = ctx.invoke(encode_secret,
                         env=env,
                         location=location,
                         keyring=keyring,
                         keyname=keyname,
                         plaintext=plaintext)

    client = secretmanager.SecretManagerServiceClient()

    path = client.secret_path(gcloud.project(env), secret_name)
    try:
        version = client.add_secret_version(path, {'data': encoded.encode('utf-8')})
    except NotFound:
        client.create_secret(client.project_path(gcloud.project(env)), secret_name, {
            'replication': {
                'automatic': {},
            },
        })
        version = client.add_secret_version(path, {'data': encoded.encode('utf-8')})

    assert version is not None
