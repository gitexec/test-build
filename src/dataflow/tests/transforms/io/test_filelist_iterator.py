from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider
from libs import GCLOUD
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from transforms.io.filelist_iterator import FileListIteratorTransform
import dill


project_id = GCLOUD.project('local')


def test_a_serialized_file_list_is_deserialized_and_processed_by_insertion_order(cloudstorage):
    with TestPipeline() as p:
        bucket = f'{project_id}-wrench-imports'
        file_seed = """
        entity_id,tree_user_id,prediction,owner,experiment_name,processing_datetime
        1fab9649-5113-4daa-a9ed-ae67bc3358b8,1038665,low,e08dc822-97b1-46f4-9154-25821286231f,lcv_level,2020-01-31T14:52:40.000+05:30
        c104b7ea-d32c-4c6e-92a5-505b3651d424,873612,zero,e08dc822-97b1-46f4-9154-25821286231f,lcv_level,2020-01-31T14:52:40.000+05:30
        """
        filename = 'wrench_test.csv'

        # Update sort_key based on the filename format
        def _sort_key(f):
            delimeter = '*'
            ts = f[f.rfind(delimeter) + 1:]
            return int(ts) if ts.isdigit() else f

        _sort_key = bytes.hex(dill.dumps(_sort_key))

        [b.delete() for b in cloudstorage.client.list_blobs(bucket)]
        cloudstorage.client.upload_blob_from_string(bucket, file_seed, filename, 'text/csv')
        p_paths = p | FileListIteratorTransform(
            env='local',
            bucket=bucket,
            files_ext='.csv',
            sort_key=_sort_key)
        assert_that(p_paths, equal_to([f'gs://{bucket}/wrench_test.csv']))


def test_runtime_serialized_file_list_is_deserialized_and_processed_by_insertion_order(cloudstorage):
    with TestPipeline() as p:
        bucket = f'{project_id}-cdc-imports'

        # Update sort_key based on the filename format
        def _sort_key(f):
            delimeter = '-'
            ts = f[f.rfind(delimeter) + 1:]
            return int(ts) if ts.isdigit() else f

        _sort_key = bytes.hex(dill.dumps(_sort_key))

        runtime_env = RuntimeValueProvider(
            option_name='env', value_type=str, default_value='local'
        )
        runtime_bucket = RuntimeValueProvider(
            option_name='bucket', value_type=str, default_value=bucket
        )
        runtime_startswith = RuntimeValueProvider(
            option_name='files_startwith', value_type=str, default_value='vibe-tree-user-statuses-final'
        )
        runtime_sort_key = RuntimeValueProvider(
            option_name='sort_key', value_type=str, default_value=_sort_key
        )
        [b.delete() for b in cloudstorage.client.list_blobs(bucket)]
        file_paths = [
            'vibe-tree-user-statuses-final-0083c-1987582612499',
            'vibe-tree-user-statuses-final-003c-1587582612405',
            'vibe-order-items-final-0030dd8697-1588231505823'
        ]
        expected_output = [
            'gs://icentris-ml-local-wbrito-cdc-imports/vibe-tree-user-statuses-final-003c-1587582612405',
            'gs://icentris-ml-local-wbrito-cdc-imports/vibe-tree-user-statuses-final-0083c-1987582612499'
        ]
        for f in file_paths:
            cloudstorage.client.upload_blob_from_string(bucket, f, f)

        p_paths = p | FileListIteratorTransform(
            env=runtime_env,
            bucket=runtime_bucket,
            files_startwith=runtime_startswith,
            sort_key=runtime_sort_key)
        assert_that(p_paths, equal_to(expected_output))
        RuntimeValueProvider.set_runtime_options(None)
