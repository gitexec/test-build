import pytest
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline

from libs import Log


@pytest.mark.skip
def test_log_dofn_does_not_error():
    with TestPipeline() as p:
        (p | 'Create Initial Collection' >> beam.Create(['one', 'two'])
           | 'Log' >> beam.ParDo(Log()))


@pytest.mark.skip
def test_log_dofn_logs_dict():
    with TestPipeline() as p:
        (p | 'Create Initial Collection' >> beam.Create([{'key': 'value', 'one': 'two'}])
           | 'Log' >> beam.ParDo(Log()))
