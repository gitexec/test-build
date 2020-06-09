import pytest
import os
from pathlib import PosixPath
from pathlib import PurePath

from airflow.models import DagBag
from libs import GCLOUD as gcloud

from libs.shared.test import BigQueryFixture
from libs.shared.test import CloudStorageFixture

import contextlib
from airflow import settings

base_path = PosixPath('/workspace/airflow/dags/')


@pytest.fixture(scope='package')
def seed(bigquery_helper):
    def _f(seeds):
        bigquery_helper.truncate(seeds)
        return bigquery_helper.seed(seeds)
    return _f


@pytest.fixture
def load_dag(tmpdir):
    def _load_and_assert(dag_id):
        nonlocal tmpdir
        tmp_directory = str(tmpdir)
        dag_bag = DagBag(dag_folder=tmp_directory, include_examples=False)

        if not isinstance(dag_id, PurePath):
            if '.py' not in dag_id:
                dag_id = '{}.py'.format(dag_id)
            dag_id = base_path / dag_id
        dag_bag.process_file(str(dag_id), only_if_updated=False)
        assert len(dag_bag.import_errors) == 0
        assert len(dag_bag.dags) == 1
        return dag_bag
    return _load_and_assert


@pytest.fixture(scope='session')
def env():
    return {
        'env': os.environ['ENV'],
        'project': gcloud.project(os.environ['ENV']),
        'user': gcloud.config()['username'],
        'client': 'bluesun'
    }


@pytest.fixture(scope='session')
def xcomm_mock():
    def s(data=None):
        class ti:
            def xcom_pull(key):
                return data

        kwargs = {'ti': ti}
        return kwargs
    return s


@pytest.fixture(scope='session')
def bigquery_helper(env):
    helper = BigQueryFixture.instance(env['env'])
    return helper


@pytest.fixture
def airflow_session():
    def session():
        return create_session()
    return session


@contextlib.contextmanager
def create_session():
    """
    Contextmanager that will create and teardown a session.
    """
    session = settings.Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@pytest.fixture
def storage(env):
    return CloudStorageFixture.instance(env['env'])
