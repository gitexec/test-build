from .gcloud import GCLOUD as gcloud
from google.cloud import secretmanager
from pyconfighelper import ConfigHelper


class Config():
    @classmethod
    def get_config(cls, env):
        helper = ConfigHelper(
            kms_project=gcloud.project(env),
            kms_location='us-west3',
            kms_key_ring='configs',
            kms_key_name='kek-{}1'.format(env[:1])
        )
        url = 'https://api.github.com/repos/iCentris/machine-learning/contents/src/configs/{}'
        url = url.format(gcloud.env(env).replace('-', '/'))
        secret = cls.secret(env, 'github')
        return helper.get_config(url, secret)

    @classmethod
    def secret(cls, env, secret_name):
        """
        https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
        """
        client = secretmanager.SecretManagerServiceClient()

        name = client.secret_version_path(gcloud.project(env), secret_name, 'latest')

        response = client.access_secret_version(name)
        return response.payload.data.decode('UTF-8')
