import pytest
from .crypto import Crypto
import json
from unittest.mock import patch, MagicMock
from .test import SecretsFixture
import os
from google.api_core.exceptions import FailedPrecondition


@pytest.fixture
def secrets():
    return SecretsFixture(os.environ['ENV'])


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def test_list_secret_versions(env, secrets):
    responses = [{
        'name': 'projects/939284484705/secrets/vibe-cdc/versions/1',
        'create_time':
            {'seconds': 1584715728,
             'nanos': 967423000
             },
        'state': 'ENABLED'
    }]
    crypto = Crypto(
        env=env['env'],
        key='vibe-cdc'
    )
    client = secrets.get_client(secrets.create_list_secrets_response(responses))
    versions = crypto.list_secret_versions(client=client)
    assert len(versions) == 1
    assert versions[0].name == 'projects/939284484705/secrets/vibe-cdc/versions/1'


def test_access_secret_version(env, secrets):
    version = '1'
    name = "vibe-cdc"
    key = '[SUPER SECRET KEY]'
    expected_response = {
        "name": name,
        "payload": {
            "data": bytes(key, 'utf-8')
        }
    }
    crypto = Crypto(
        env=env['env'],
        key=key
    )
    client = secrets.get_client(secrets.create_access_secrets_response(expected_response))
    secret = crypto.access_secret_version(version=version, client=client)
    assert secret == key


def test_access_destroyed_secret_version(env, secrets):
    version = '2'
    key = '[SUPER SECRET KEY]'
    expected_response = FailedPrecondition('[projects/75936188733/secrets/vibe-cdc/versions/2] is in DESTROYED state')
    crypto = Crypto(
        env=env['env'],
        key=key
    )
    client = secrets.get_client(expected_response)
    secret = crypto.access_secret_version(version=version, client=client)

    if secret is not None:
        assert False


def test_list_secret_keys(env, secrets):
    list_secret_versions_response = [
        Struct(**{
            'name': 'projects/939284484705/secrets/vibe-cdc/versions/1',
            'create_time':
            {'seconds': 1584715728,
             'nanos': 967423000
             },
            'state': 'ENABLED'  # Three options available for a secret - ENABLED, DISABLED, DESTROYED
        }),
        Struct(**{
            'name': 'projects/939284484705/secrets/vibe-cdc/versions/2',
            'create_time':
            {'seconds': 1584715728,
             'nanos': 967423000
             },
            'state': 'ENABLED'
        })
    ]

    good_key = '6L53g000h6WjSsuzTb3pE4yp7LzueVViIw6enaktLPM='
    bad_key = 'A REALLY REALLY MESSED UP KEY'
    bad_key_response = secrets.create_access_secrets_response({
        'name': 'bad key',
        'payload': {
            'data': bytes(bad_key, 'utf-8')
        }
    })
    good_key_response = secrets.create_access_secrets_response({
        'name': 'good key',
        'payload': {
            'data': bytes(good_key, 'utf-8')
        }
    })
    crypto = Crypto(
        env=env['env'],
        key='vibe-cdc'
    )
    with patch.object(crypto, 'list_secret_versions', return_value=list_secret_versions_response):
        with patch.object(crypto, 'access_secret_version', side_effect=[good_key_response, bad_key_response]):
            arr = list(crypto.list_secret_keys(client=MagicMock()))
            assert arr[0].payload.data.decode('utf-8') == good_key
            assert arr[1].payload.data.decode('utf-8') == bad_key


def test_exipired_key_rotation(env, secrets):
    crypto = Crypto(
        env=env['env'],
        key='vibe-cdc'
    )
    good_key = '6L53g000h6WjSsuzTb3pE4yp7LzueVViIw6enaktLPM='
    bad_key = 'A REALLY REALLY MESSED UP KEY'
    payload = {
        'id': 100, 'category_name': 'phone', 'user_id': None, 'status': None, 'created_at': '2019-01-26T17:34:26.000Z',
        'updated_at': '2020-04-02T09:32:47.000Z', 'contacts_count': 8629539, 'icentris_client': 'worldventures'
    }
    keys = [
        bad_key,
        good_key
    ]
    enc = crypto.encrypt(raw=json.dumps(payload), key=good_key)  # `good_key` called
    dec = json.loads(crypto.decrypt(enc=enc, keys=keys))  # `bad_key` then `good_key` called
    assert payload == dec
