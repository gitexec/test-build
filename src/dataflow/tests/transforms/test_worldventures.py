import apache_beam as beam
# import pytest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from transforms.worldventures import WorldVenturesStagingUsersTransform
from transforms.worldventures import WorldVenturesStagingOrdersTransform
from transforms.worldventures import WorldVenturesNormalizeOrderType
from transforms.worldventures import WorldVenturesNormalizeUserType
from libs.shared.test_factories import StagingUserFactory
from libs.shared.test_factories import LakeTreeOrderFactory
from libs.shared.test_factories import LakeTreeOrderStatusFactory
from libs.shared.test_factories import LakeTreeUserFactory
from libs.shared.test_factories import FactoryRegistry


client = 'worldventures'


def test_client_type_missing_skips_record():
    user = StagingUserFactory(icentris_client=client)
    del user['client_type']

    with TestPipeline() as p:
        pcoll = (p | beam.Create([user])
                   | WorldVenturesStagingUsersTransform())

        assert_that(pcoll, equal_to([]))


def test_client_type_is_none_skips_record():
    user = StagingUserFactory(icentris_client=client)
    user['client_type'] = None

    with TestPipeline() as p:
        pcoll = (p | beam.Create([user])
                   | WorldVenturesStagingUsersTransform())

        assert_that(pcoll, equal_to([]))


def test_client_type_is_empty_string_skips_record():
    user = StagingUserFactory(icentris_client=client)
    user['client_type'] = ''

    with TestPipeline() as p:
        pcoll = (p | beam.Create([user])
                   | WorldVenturesStagingUsersTransform())

        assert_that(pcoll, equal_to([]))


def test_normalize_user_type_params():
    if 'LakeTreeUserFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeUserFactory']

    FactoryRegistry.create(LakeTreeUserFactory, icentris_client=client)
    user = FactoryRegistry.registry['LakeTreeUserFactory'][0]
    user['foo'] = 'Distributor'

    with TestPipeline() as p:
        pcoll = (p | beam.Create([user])
                   | beam.ParDo(WorldVenturesNormalizeUserType(in_name='foo', out_name='bar'))
                   | beam.Map(lambda x: {'bar': x['bar']}))
        assert_that(pcoll, equal_to([{'bar': 'Distributor'}]))


def test_client_status_defaults_to_inactive():
    user = StagingUserFactory(icentris_client=client)
    user['client_status'] = 'FooBar'

    with TestPipeline() as p:
        pcoll = (p | beam.Create([user])
                   | WorldVenturesStagingUsersTransform()
                   | beam.Map(lambda x: {'status': x['status']}))

        assert_that(pcoll, equal_to([{'status': 'Inactive'}]))


def test_order_type_distributor_wholesale():
    if 'LakeTreeUserFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeUserFactory']

    if 'LakeTreeOrderFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeOrderFactory']

    FactoryRegistry.create(LakeTreeUserFactory, icentris_client=client)
    FactoryRegistry.create(LakeTreeOrderFactory, icentris_client=client)
    order = FactoryRegistry.registry['LakeTreeOrderFactory'][0]
    order['type'] = 'Distributor'
    order['created'] = '2020-04-17 14:03:50'
    order['order_date'] = '2020-04-17 14:03:50'

    with TestPipeline() as p:
        pcoll = (p | beam.Create([order])
                   | beam.ParDo(WorldVenturesNormalizeOrderType())
                   | beam.Map(lambda x: {'type': x['type']}))
        assert_that(pcoll, equal_to([{'type': 'Wholesale'}]))


def test_order_type_distributor_autoship():
    if 'LakeTreeUserFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeUserFactory']

    if 'LakeTreeOrderFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeOrderFactory']

    FactoryRegistry.create(LakeTreeUserFactory, icentris_client=client)
    FactoryRegistry.create(LakeTreeOrderFactory, icentris_client=client)
    order = FactoryRegistry.registry['LakeTreeOrderFactory'][0]
    order['type'] = 'Distributor'
    order['created'] = '2020-05-17 14:03:50'
    order['order_date'] = '2020-04-17 14:03:50'

    with TestPipeline() as p:
        pcoll = (p | beam.Create([order])
                   | beam.ParDo(WorldVenturesNormalizeOrderType())
                   | beam.Map(lambda x: {'type': x['type']}))
        assert_that(pcoll, equal_to([{'type': 'Autoship'}]))


def test_order_type_autoship():
    if 'LakeTreeUserFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeUserFactory']

    if 'LakeTreeOrderFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeOrderFactory']

    FactoryRegistry.create(LakeTreeUserFactory, icentris_client=client)
    FactoryRegistry.create(LakeTreeOrderFactory, icentris_client=client)
    order = FactoryRegistry.registry['LakeTreeOrderFactory'][0]
    order['type'] = 'Autoship'

    with TestPipeline() as p:
        pcoll = (p | beam.Create([order])
                   | beam.ParDo(WorldVenturesNormalizeOrderType())
                   | beam.Map(lambda x: {'type': x['type']}))
        assert_that(pcoll, equal_to([{'type': 'Autoship'}]))


def test_orders_transform():
    if 'LakeTreeUserFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeUserFactory']

    if 'LakeTreeOrderFactory' in FactoryRegistry.registry:
        del FactoryRegistry.registry['LakeTreeOrderFactory']

    FactoryRegistry.create(LakeTreeUserFactory, icentris_client=client)
    FactoryRegistry.create(LakeTreeOrderFactory, icentris_client=client)
    FactoryRegistry.create(LakeTreeOrderStatusFactory, icentris_client=client)
    order = FactoryRegistry.registry['LakeTreeOrderFactory'][0]
    status = FactoryRegistry.registry['LakeTreeOrderStatusFactory'][0]
    order['sponsor_id'] = None
    order['client_user_type'] = 'Distributor'
    order['client_status'] = status['description']
    order['created'] = '2020-04-17 14:03:50'
    order['order_date'] = '2020-04-17 14:03:50'

    with TestPipeline() as p:
        pcoll = (p | beam.Create([order])
                   | WorldVenturesStagingOrdersTransform()
                   | beam.Map(lambda x: {'type': x['type']}))
        assert_that(pcoll, equal_to([{'type': 'Wholesale'}]))
