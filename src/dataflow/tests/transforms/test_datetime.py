import apache_beam as beam
from datetime import datetime
from dateutil import parser
from google.cloud import bigquery as bq

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
# from apache_beam.transforms import Map

from transforms.datetime import StringifyDatetimes, StringToDatetime, InsertIngestionTimestamp


def test_transforms_dates():
    with TestPipeline() as p:
        date = parser.parse('2019-01-01').date()
        pcoll = p | beam.Create([{'date': date}])
        date_coll = pcoll | beam.ParDo(StringifyDatetimes())
        assert_that(date_coll, equal_to([{'date': str(date)}]), label='assert:date')


def test_transforms_datetimes():
    with TestPipeline() as p:
        dt = parser.parse('2019-01-01 04:59:59')
        pcoll = p | beam.Create([{'date': dt, 'date2': dt}])
        date_coll = pcoll | beam.ParDo(StringifyDatetimes())
        assert_that(date_coll, equal_to([{'date': str(dt), 'date2': str(dt)}]), label='assert:datetime')


def test_transforms_nested_fields():
    with TestPipeline() as p:
        dt = datetime.now()
        pcoll = p | beam.Create([{'date': dt.date(),
                                  'children': [{'datetime': dt,
                                                'children': [{'datetime': dt}, {'datetime': dt}]},
                                               {'datetime': '0000-00-00 00:00:00'},
                                               {'date': '0000-00-00'}
                                               ]}])

        date_coll = pcoll | beam.ParDo(StringifyDatetimes())
        assert_that(date_coll, equal_to([{'date': str(dt.date()),
                                          'children': [{'datetime': str(dt),
                                                        'children': [{'datetime': str(dt)}, {'datetime': str(dt)}]},
                                                       {'datetime': None},
                                                       {'date': None}
                                                       ]}]), label='assert:nested')


def test_transform_string_to_datetime():
    with TestPipeline() as p:
        schema = [bq.schema.SchemaField('date1', 'DATETIME', 'REQUIRED', None, ()),
                  bq.schema.SchemaField('date2', 'DATETIME', 'REQUIRED', None, ())]
        ex1 = '2019-01-26T17:34:26.000Z'
        fmt = '%Y-%m-%d %H:%M:%S'
        expected = parser.parse(ex1).strftime(fmt)
        pcoll = p | beam.Create([{'schema': schema, 'payload': {'date1': ex1, 'date2': ex1}}])
        date_coll = pcoll | beam.ParDo(StringToDatetime(fmt))
        assert_that(date_coll, equal_to([{'date1': expected, 'date2': expected}]), label='assert:datetime')


def test_transform_datestring_to_datetime():
    with TestPipeline() as p:
        schema = [bq.schema.SchemaField('date1', 'DATE', 'REQUIRED', None, ()),
                  bq.schema.SchemaField('date2', 'DATE', 'REQUIRED', None, ())]
        ex1 = '2020-01-31T14:52:40.000+05:30'
        fmt = '%Y-%m-%d'
        expected = parser.parse(ex1).strftime(fmt)
        pcoll = p | beam.Create([{'schema': schema, 'payload': {'date1': ex1, 'date2': ex1}}])
        date_coll = pcoll | beam.ParDo(StringToDatetime(fmt))
        assert_that(date_coll, equal_to([{'date1': expected, 'date2': expected}]), label='assert:datetime')


def test_transform_string_to_timestamp():
    with TestPipeline() as p:
        schema = [bq.schema.SchemaField('date1', 'TIMESTAMP', 'REQUIRED', None, ()),
                  bq.schema.SchemaField('date2', 'TIMESTAMP', 'REQUIRED', None, ())]
        ex1 = '2019-01-26T17:34:26.000Z'
        fmt = '%Y-%m-%d %H:%M:%S'
        expected = parser.parse(ex1).timestamp()
        pcoll = p | beam.Create([{'schema': schema, 'payload': {'date1': ex1, 'date2': ex1}}])
        date_coll = pcoll | beam.ParDo(StringToDatetime(fmt))
        assert_that(date_coll, equal_to([{'date1': expected, 'date2': expected}]), label='assert:timestamp')


def test_transform_string_to_timestamp_record():
    with TestPipeline() as p:
        schema = [bq.schema.SchemaField('dates', 'RECORD', 'REPEATED', None, (bq.schema.SchemaField(
            'date1', 'DATETIME', 'REQUIRED', None, ()), bq.schema.SchemaField('date2', 'DATETIME', 'REQUIRED', None, ())))]
        dt = '2019-01-26T17:34:26.000Z'
        fmt = '%Y-%m-%d %H:%M:%S'
        expected_dt = parser.parse(dt).strftime(fmt)
        pcoll = p | beam.Create([{'schema': schema, 'payload': {'dates': [{'date1': dt}, {'date2': dt}]}}])

        date_coll = pcoll | beam.ParDo(StringToDatetime(fmt))
        assert_that(date_coll, equal_to([{'dates': [{'date1': expected_dt},
                                                    {'date2': expected_dt}
                                                    ]}]), label='assert:nested')


def test_current_datetime_is_inserted_in_pcoll_element():
    with TestPipeline() as p:
        pcoll = p | beam.Create([{}])

        ts_coll = pcoll | beam.ParDo(InsertIngestionTimestamp()) | beam.FlatMap(dict.keys)
        assert_that(ts_coll, equal_to(['ingestion_timestamp']))
