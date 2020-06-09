import gzip

import os
import base64
import logging
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from .gcloud import GCLOUD as gcloud
from google.api_core.exceptions import FailedPrecondition


log = logging.getLogger()


class Crypto:
    def __init__(self, env, key):
        self._env = env
        self._key = key

    def _unpad(self, s): return s[:-ord(s[len(s)-1:])]

    def generate_key(self):
        pass

    def list_secret_versions(self, client):
        project = gcloud.project(self._env)
        parent = client.secret_path(project, self._key)
        return list(client.list_secret_versions(parent))

    def access(self, client, name):
        try:
            response = client.access_secret_version(name)
            return response.payload.data.decode('UTF-8').rstrip()
        except FailedPrecondition as e:
            log.error('Failed accessing key by name:%s \n%s', name, e)
            return None

    def access_secret_version(self, client, version=None, name=None):
        if name is not None:
            return self.access(client, name)
        else:
            project = gcloud.project(self._env)
            name = client.secret_version_path(project, self._key, version)
            return self.access(client, name)

    def encrypt(self, raw, key, encoding='utf-8'):
        backend = default_backend()
        iv = os.urandom(16)
        compressed = gzip.compress(bytes(raw, encoding=encoding))
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(compressed) + padder.finalize()
        cipher = Cipher(algorithms.AES(base64.b64decode(key)), modes.CBC(iv), backend=backend)
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(padded_data) + encryptor.finalize()
        return f'{bytes.hex(iv)}:{bytes.hex(encrypted)}'

    def decrypt(self, enc, keys):
        for key in keys:
            try:
                backend = default_backend()
                iv, message = enc.split(':')
                iv = bytes.fromhex(iv)
                message = bytes.fromhex(message)
                cipher = Cipher(algorithms.AES(base64.b64decode(key)), modes.CBC(iv), backend=backend)
                decryptor = cipher.decryptor()
                padded = decryptor.update(message)
                compressed = self._unpad(padded)
                data = gzip.decompress(compressed)
                return data
            except Exception as e:
                print(e)
                pass

        raise Exception('Unable to decrypt.  Tried every key to decrypt this payload.')

    def list_secret_keys(self, client):
        for secret in self.list_secret_versions(client=client):
            key = self.access_secret_version(name=secret.name, client=client)
            if key:
                yield key
