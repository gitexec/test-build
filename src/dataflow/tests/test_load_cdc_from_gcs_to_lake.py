import pytest
import apache_beam as beam
from libs import GCLOUD as gcloud, Crypto
import datetime
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from load_cdc_from_gcs_to_lake import RecordDecryption, Runner, RuntimeOptions


project_id = gcloud.project('local')
bucket = f'{project_id}-cdc-imports'
# TODO: Move test data to Factory wbrito 05/05/2020
file_seed = 'eee7cb05d80b8a58bccff2f4290b49ff:b42e122e33a81939c5810d44ce92830e431ea45689\
f0aa7ba6c8cd04f2049bfa2fa92283c6e7fb12458a8e910873a2322e8f2d18f6ae7f728f715a4c633b73328d12b0260\
f6b49b84e46c127c58acf71e81b5847318f5eea3293c304ea38090de987c24debf2f8c282920ac82bfc5bd473a9002c9\
162bfa08f1eb309e46dc7e7ca1964334620c8581f028228e8b3edc4233c0dca5edf7aea2a00a96ef272c5061f9b47f2e3\
edf5a52a26fa6f5976e3a12c27c4a0cd7ef8ec401cd8755ac4566f674aa3a33a22111e55bd7d76aa32e8911612971e1aa0\
7ad09e21323be4d47972d94666ecb48271f8fe44be7b6b2b2508fcffdf34c0e7a9cec72aee07252469c4dacc060a4dadedae\
4fd28679e047f2b6394fb668e021e79fe0f414e2a15a0a652f3f628431df1463da2d61d0f1211a121c5ea715d3ac3fe22cd11b\
97933c3f337b8224261caf321f667a6c894b09eba6a13b9a155be69b903d3c51fc8507a7fc43e2b9145bb8e53826c78ef9645b8\
3e811913e365eb9daee0e732fd6fe3bc0fc9f6eebe1ca0297f7fc98f8b5075a28283cd3f17ff0482116aaa0b2e3d7d8bed976f4b\
82e50efb2627728cb76601819b4aa69b59eee86348b4d51540a8659da87f630468344c0212f393011345668fd0f4afbef1f36cf7\
b94815a688aa8754fa9a4d9942bc14de16e2fdf712b72a9b9e0e57bbe27df145f75a554806e24ee3c34bf76d1da247d653cca9507\
b3d13e43afbb94e5e8915559541d6a2d1822a9a5ba8d1157477fceb13687852e69d1a3cc40253727a7e0480fd303005059450cd381\
f8ea427887ac591665ff4266d3f5ae93e6c11c6553cb7220a695c04ebceac7369dc0fd8e1a68ab82b23099db224511ac9320e60a684\
b656f23cab924ec795662b9c3e94c65d2b076d856483a89c1f3f87a743c07acf6eb0824d1834abf14c50717343138d67d7e8171f4cac7\
71f71bc22a330dad96a62ac8a9354992d622c644def96b7c2e3ea550a2648383d3900faff5e74cad261998749faf42f595d24c3f342dd3c\
6b38078d35c9d5c339048b234a11f022c21d46194a0935a74cf366b9862389d2fe5203b9243ccee8b096ee3196411d49515f0f3384a906d91325a7f80'
filename = 'vibe-tree-users-final-8b8e336ea2366596964f1ed2c67c3039bc5cfe57e823db8e647835f1fee26040-1587028509211'
# TODO: Move to Factory wbrito 05/05/2020
expected_dec_seed = [{
    'id': 486859, 'client_user_id': '36608801', 'first_name': 'Thodoris', 'last_name': 'Polyrakis',
    'company_name': '', 'screen_name': '36608801', 'user_type_id': 2, 'user_status_id': 2, 'email': 'tpolyrakis@gmail.com',
    'phone': '', 'mobile_phone': '6977707798', 'fax': '', 'address1': 'Eleftheriou Venizelou 92-94', 'address2': '',
    'city': 'CHANIA,', 'state': '94', 'zip': '73100', 'country': 'GR', 'county': '',
    'mail_address1': 'Eleftheriou Venizelou 92-94.', 'mail_address2': '', 'mail_city': 'CHANIA,', 'mail_state': '94',
    'mail_zip': '73100', 'mail_country': 'GR', 'mail_county': '', 'other_address1': None, 'other_address2': None,
    'other_city': None, 'other_state': None, 'other_zip': None, 'other_country': None, 'other_county': None,
    'preferred_language': '312', 'rank_id': 250, 'paid_rank_id': 50, 'parent_id': 486634, 'sponsor_id': 470559,
    'birth_date': '1981-04-17T00:00:00.000Z', 'gender': 'Male', 'field1': '1', 'field2': 'thodorispolyrakis',
    'field3': None, 'field4': None, 'field5': None, 'date1': None, 'date2': None, 'date3': None, 'date4': None,
    'date5': None, 'created_date': '2012-05-07T05:00:00.000Z', 'coded_date': None,
    'updated_date': '2020-04-17T10:29:08.000Z', 'birth_month_day': 417, 'anniversary_month_day': 507, 'date6': None,
    'date7': None, 'date8': None, 'date9': None, 'date10': None, 'int_field1': 1, 'int_field2': None,
    'int_field3': None, 'int_field4': None, 'int_field5': None, 'int_field6': None, 'int_field7': None,
    'int_field8': None, 'int_field9': None, 'int_field10': 1, 'client_parent_id': '36577201',
    'client_sponsor_id': '34205501', 'checksum': None, 'client_rank_id': '25', 'field6': None, 'field7': None,
    'field8': None, 'field9': None, 'field10': None, 'field11': 'API - Api_User1', 'field12': '****8729',
    'field13': None, 'field14': None, 'field15': None, 'lat': None, 'lon': None, 'address3': None, 'suburb': None,
    'mail_address3': None, 'mail_suburb': None, 'other_address3': None, 'other_suburb': None, 'children_count': 0,
    'icentris_client': 'worldventures',
    'ingestion_timestamp': 'Fri Apr 17 2020 10:30:03 GMT+0000 (Coordinated Universal Time)',
    'leo_eid': 'z/2020/04/17/10/29/1587119396615-0000025'
}]

file_seed2 = 'b5a6a555c2ceff9d90655e83a989491e:\
e6fc7acd34f90fffafb2e1707ff3d801c954db15e4688b066108e47ef1\
ff8b01c371f1db51b4fb330f759f56acf8c86324d73ee6ca343f866c70f\
b4e61684257851f0ff7f956a70ecc8a61a59ba73d17504468bc956fa669\
a907b2ed85bc7f5e3d394f160810cb3a5417ae7a1342317da3a0ac7567f\
6adc6286017d68444823b0246fa70f8b3c60d32a748d7a92823813beb53\
179419eb91634a88bcfbaa49b7f7f34b597e762779135eda7dd30eb600'


@pytest.fixture(scope='module')
def run(env, run_pipeline, bigquery, cloudstorage):
    start = datetime.datetime.now()
    lake_table = 'lake.pyr_message_recipients'
    dest = f'{project_id}:{lake_table}'
    files_startwith = 'vibe-message-recipients-final'

    [b.delete() for b in cloudstorage.client.list_blobs(bucket)]
    cloudstorage.client.upload_blob_from_string(bucket, file_seed, filename)
    run_pipeline(
        Runner,
        RuntimeOptions([
            '--env',
            env['env'],
            '--files_startwith',
            files_startwith,
            '--dest',
            dest]),
        [])
    rs = bigquery.query(f'SELECT * FROM {lake_table} WHERE ingestion_timestamp >= "{start}"')
    yield rs


def test_record_decryption_can_decrypt(env, secrets):
    crypto = Crypto(
        env=env['env'],
        key='vibe-cdc'
    )
    keys = ['6L53g000h6WjSsuzTb3pE4yp7LzueVViIw6enaktLPM=']
    expected = {'id': 1, 'category_name': 'phone', 'user_id': None,
                'status': None, 'created_at': '2019-01-26T17:34:26.000Z',
                'updated_at': '2020-04-02T09:32:47.000Z', 'contacts_count': 8629539,
                'icentris_client': 'worldventures'}
    with TestPipeline() as p:
        pcoll = (p | beam.Create([file_seed2])
                 | beam.ParDo(RecordDecryption(env=env['env'], crypto=crypto, keys=keys)))
    assert_that(pcoll, equal_to([expected]))


@pytest.mark.skip(reason='Causes everything else to fail')
def test_the_df_template_load_and_insert_record_into_bq(run):
    assert len(run) == 1
