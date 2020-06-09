import logging
import pytest
import os

from libs import GCLOUD as gcloud
from libs.shared.test import BigQueryFixture, CloudStorageFixture, SecretsFixture

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider


logger = logging.getLogger()


@pytest.fixture(scope='session')
def sql_templates_path():
    return '/workspace/dataflow/tests/sql_templates'


@pytest.fixture(scope='session')
def bigquery(env):
    helper = BigQueryFixture.instance(env['env'])
    return helper


@pytest.fixture(scope='session')
def cloudstorage(env):
    return CloudStorageFixture.instance(env['env'])


@pytest.fixture(scope='session')
def secrets(env):
    return SecretsFixture(env['env'])


@pytest.fixture(scope='session')
def env():
    return {
        'env': os.environ['ENV'],
        'project': gcloud.project(os.environ['ENV']),
        'user': gcloud.config()['username'],
        'client': 'bluesun'
    }


@pytest.fixture(scope='module')
def run_pipeline(bigquery):
    def _r(runner, options, seeds):
        bigquery.truncate(seeds)
        bigquery.seed(seeds)

        RuntimeValueProvider.set_runtime_options(None)

        runner._run(TestPipeline(options=options), options)

    return _r
