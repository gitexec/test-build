"""
jeeves config edit [dev,tst,prd]
"""
from cryptography.fernet import Fernet
from google.cloud import kms_v1 as kms
import click
import json
import os
from pathlib import PosixPath
from subprocess import call
import tempfile
from jeeves.cli import pass_environment
from jeeves.commands.libs import GCLOUD as gcloud


@click.group()
@pass_environment
def cli(ctx):
    pass


def paths(env):
    if env != 'prd':
        env = '/'.join(gcloud.project(env).split('ml-')[1].split('-'))
    path = PosixPath('/workspace/configs')
    return {'dek_path': path / env / 'dek.enc',
            'config_path': path / env / 'config.enc',
            'path': path / env}


def _create_dek():
    return Fernet.generate_key()


def _get_kek_path(client, env):
    return client.crypto_key_path_path(gcloud.project(env), 'us-west3', 'configs',
                                       'kek-'+env[0][:1]+'1')


def _write_dek(client, env, plaintext):
    response = client.encrypt(_get_kek_path(client, env), plaintext)
    with paths(env)['dek_path'].open(mode='wb') as f:
        f.write(response.ciphertext)


def _read_dek(client, env):
    path = paths(env)['dek_path']
    if path.is_file():
        with path.open(mode='rb') as f:
            ciphertext = f.read()
        dek = client.decrypt(_get_kek_path(client, env), ciphertext)
        return dek.plaintext
    return None


def _write_config(env, dek, text):
    f = Fernet(dek)
    encrypted = f.encrypt(text.encode('utf-8'))

    with paths(env)['config_path'].open(mode='wb',) as f:
        f.write(encrypted)


def _read_config(env, dek):
    path = paths(env)['config_path']
    if path.is_file():
        with path.open(mode='rb') as f:
            contents = f.read()
            f = Fernet(dek)
            config = f.decrypt(contents)
            config = json.dumps(json.loads(config), sort_keys=True, indent=2)
    else:
        config = json.dumps({'aws': {'key': '', 'secret': ''}}, sort_keys=True, indent=2)

    return config


def _run_vim(config):
    editor = os.environ.get('EDITOR', 'vim')
    with tempfile.NamedTemporaryFile(suffix=".json") as tf:
        tf.write(config.encode('utf-8'))
        tf.flush()
        call([editor, tf.name])

        # do the parsing with `tf` using regular File operations.
        # for instance:
        tf.seek(0)
        new_config = tf.read().decode('utf-8')

    try:
        json.loads(new_config)
        confirm = input('Save new file? y/n')

        if (confirm == 'y' or confirm == 'yes'):
            return new_config
        else:
            return
    except ValueError:
        edit = input('Invalid json. (e)dit or (c)ancel?')

        if (edit == 'edit' or edit == 'e'):
            return _run_vim(new_config)
        else:
            return


@cli.command()
@click.argument('env')
@pass_environment
def edit(ctx, env):
    client = kms.KeyManagementServiceClient()
    # Fetch decrypted config file
    dek = _read_dek(client, env)
    config = _read_config(env, dek)

    # Edit config in vim
    new_config = _run_vim(config)

    if not new_config:
        return

    # Create new dek
    new_dek = _create_dek()

    # Write encrypted key and file
    paths(env)['path'].mkdir(parents=True, exist_ok=True)
    _write_config(env, new_dek, new_config)
    _write_dek(client, env, new_dek)

    print("File successfully edited and saved. Don't forget to commit your changes")
