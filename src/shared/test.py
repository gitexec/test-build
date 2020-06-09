import logging
import os
import pytest
from google.cloud import bigquery
from .gcloud import GCLOUD as gcloud
from .bigquery import BigQuery
from .storage import CloudStorage
from unittest import mock
import google.cloud.storage._http
from google.cloud.exceptions import NotFound
from google.cloud import secretmanager_v1
from google.cloud.secretmanager_v1.proto import service_pb2


class BigQueryFixture():
    me = None

    @classmethod
    def instance(cls, env):
        if cls.me is None:
            cls.me = BigQueryFixture(env)
        return cls.me

    def __init__(self, env):
        project = gcloud.project(env)
        self._bq = BigQuery.instance(env)
        self._datasets = {'bluesun': bigquery.Dataset('{}.{}'.format(project, 'pyr_bluesun_{}'.format(env))),
                          'pii': bigquery.Dataset('{}.{}'.format(project, 'pii')),
                          'lake': bigquery.Dataset('{}.{}'.format(project, 'lake')),
                          'system': bigquery.Dataset('{}.{}'.format(project, 'system')),
                          'staging': bigquery.Dataset('{}.{}'.format(project, 'staging')),
                          'warehouse': bigquery.Dataset('{}.{}'.format(project, 'warehouse'))}
        self._tables = []

    def query(self, sql):
        return self._bq.query(sql)

    def seed(self, seeds):
        queue = self._bq.create_queue()
        for dataset, tables in seeds:
            ds = self._datasets[dataset]
            for tbl, rows in tables:
                if len(rows) > 0:
                    builder = BigQuery.querybuilder(
                        table=f'{ds.dataset_id}.{tbl}',
                        insert=rows
                    )
                    queue.append(str(builder))
        try:
            queue.run()
        except Exception:
            logging.getLogger().error(queue.errors)
            raise

    def truncate(self, seeds):
        queues = [
                # self._bq.create_queue(self._bq.client.delete_table),
                # self._bq.create_queue(self._bq.client.copy_table),
                # self._bq.create_queue(self._bq.client.delete_table),
                # self._bq.create_queue(self._bq.client.copy_table),
                self._bq.create_queue(self._bq._query)
        ]
        for dataset, tables in seeds:
            ds = self._datasets[dataset]

            for tbl, rows in tables:
                # orig_tbl_id = f'{self._bq.client.project}.{ds.dataset_id}.{tbl}'
                # cp_tbl_id = orig_tbl_id + '_cp'
                # queues[0].append(cp_tbl_id, not_found_ok=True)
                # queues[1].append(orig_tbl_id, cp_tbl_id)
                # queues[2].append(orig_tbl_id, not_found_ok=True)
                # queues[3].append(cp_tbl_id, orig_tbl_id)
                queues[0].append(f'DELETE FROM `{ds.dataset_id}.{tbl}` WHERE 1=1')

        try:
            for i, q in enumerate(queues):
                q.run()
        except Exception:
            # print(queues[0].errors)
            logging.getLogger().error(queues[0].errors)
            raise


class CloudStorageFixture():
    me = None

    @classmethod
    def instance(cls, env):
        if cls.me is None:
            cls.me = CloudStorageFixture(env)
        return cls.me

    def __init__(self, env):
        project = gcloud.project(env)
        self.client = CloudStorage(project)

    @classmethod
    def mock_connection(self, *responses):
        mock_conn = mock.create_autospec(google.cloud.storage._http.Connection)
        mock_conn.user_agent = 'testing 1.2.3'
        mock_conn.api_request.side_effect = list(responses) + [NotFound('miss')]
        return mock_conn


class SecretsFixture():

    def __init__(self, env):
        self.env = env

    @classmethod
    def get_client(cls, expected_response):
        # Mock the API response
        channel = ChannelStub(responses=[expected_response])
        patch = mock.patch("google.api_core.grpc_helpers.create_channel")
        with patch as create_channel:
            create_channel.return_value = channel
            return secretmanager_v1.SecretManagerServiceClient()

    @classmethod
    def create_list_secrets_response(cls, responses):
        next_page_token = ""
        total_size = 705419236
        expected_response = {
            "next_page_token": next_page_token,
            "total_size": total_size,
            "versions": responses,
        }
        return service_pb2.ListSecretVersionsResponse(**expected_response)

    @classmethod
    def create_access_secrets_response(cls, response):
        return service_pb2.AccessSecretVersionResponse(**response)


class ChannelStub(object):
    """Stub for the grpc.Channel interface."""

    def __init__(self, responses=[]):
        self.responses = responses
        self.requests = []

    def unary_unary(self, method, request_serializer=None, response_deserializer=None):
        return MultiCallableStub(method, self)


class MultiCallableStub(object):
    """Stub for the grpc.UnaryUnaryMultiCallable interface."""

    def __init__(self, method, channel_stub):
        self.method = method
        self.channel_stub = channel_stub

    def __call__(self, request, timeout=None, metadata=None, credentials=None):
        self.channel_stub.requests.append((self.method, request))

        response = None
        if self.channel_stub.responses:
            response = self.channel_stub.responses.pop()

        if isinstance(response, Exception):
            raise response

        if response:
            return response


skipif_prd = pytest.mark.skipif(os.environ.get('ENV', 'prd') == 'prd', reason='Skipped for production environment')
