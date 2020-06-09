# https://cloud.google.com/storage-transfer/docs/create-client
# https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program

import os
import json
import googleapiclient.discovery


def create_job(
    description,
    project_id,
    access_key_id,
    secret_access_key,
    start_datetime,
    source_bucket,
    source_path,
    sink_bucket
):
    """Create a one-time transfer from Amazon S3 to Google Cloud Storage."""
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    if (source_path):
        object_conditions = {
            'include_prefixes': [source_path]
        }
    else:
        object_conditions = None

    # Edit this template with desired parameters.
    transfer_job = {
        'description': description,
        'status': 'ENABLED',
        'projectId': project_id,
        'schedule': {
            'scheduleStartDate': {
                'day': start_datetime.day,
                'month': start_datetime.month,
                'year': start_datetime.year,
            },
            'scheduleEndDate': {
                'day': start_datetime.day,
                'month': start_datetime.month,
                'year': start_datetime.year,
            },
            'startTimeOfDay': {
                'hours': start_datetime.hour,
                'minutes': start_datetime.minute,
                'seconds': start_datetime.second,
            },
        },
        'transferSpec': {
            'awsS3DataSource': {
                'bucketName': source_bucket,
                'awsAccessKey': {
                    'accessKeyId': access_key_id,
                    'secretAccessKey': secret_access_key,
                },
            },
            'object_conditions': object_conditions,
            'gcsDataSink': {'bucketName': sink_bucket},
        }
    }

    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))


def main(data, context):
    import datetime

    if 'attributes' in data:
        source_bucket = data['attributes']['source_bucket']
        sink_bucket = data['attributes']['sink_bucket']
        source_path = data['attributes']['source_path']
    else:
        raise ValueError('"attributes" key not found in data parameter.')

    print('data: {}'.format(data))

    if 'GCP_PROJECT' in os.environ:
        project_id = os.environ['GCP_PROJECT']
    else:
        raise ValueError('GCP_PROJECT not found in environment. Aborted')

    # We should pull secrets from Cloud KMS
    if 'ACCESS_KEY_ID' in os.environ:
        access_key_id = os.environ['ACCESS_KEY_ID']
    else:
        raise ValueError('ACCESS_KEY_ID not found in environment. Aborted')

    if 'SECRET_ACCESS_KEY' in os.environ:
        secret_access_key = os.environ['SECRET_ACCESS_KEY']
    else:
        raise ValueError('SECRET_ACCESS_KEY not found in environment. Aborted')

    description = 'Transfer S3 "{}{}" to GC "{}"'.format(
        source_bucket, '/' + source_path if source_path else '', sink_bucket
    )

    start_datetime = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)

    print('description: {}'.format(description))
    print('project_id: {}'.format(project_id))
    print('start_datetime: {}'.format(start_datetime))
    print('Call create_job')

    create_job(
        description,
        project_id,
        access_key_id,
        secret_access_key,
        start_datetime,
        source_bucket,
        source_path,
        sink_bucket
    )


if __name__ == '__main__':
    """
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    This block only executes when you're running this script directly.
    It's purpose is to setup the 'data' argument for the main function in
    the way it would be passed to a Cloud Function.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('project_id', help='Your Google Cloud project ID.')
    parser.add_argument('access_key_id', help='Your AWS access key id.')
    parser.add_argument('secret_access_key', help='Your AWS secret access '
                        'key.')
    parser.add_argument('source_bucket', help='AWS source bucket name.')
    parser.add_argument('source_path', help='AWS path relative to src bucket.')
    parser.add_argument('sink_bucket', help='GCS sink bucket name.')

    args = parser.parse_args()

    # Set up environment using arg values
    os.environ['GCP_PROJECT'] = args.project_id
    os.environ['ACCESS_KEY_ID'] = args.access_key_id
    os.environ['SECRET_ACCESS_KEY'] = args.secret_access_key

    data = {
        'attributes': {
            'source_bucket': args.source_bucket,
            'sink_bucket': args.sink_bucket,
            'source_path': args.source_path
        }
    }

    main(data, None)
