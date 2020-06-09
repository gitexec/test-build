from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'staging'
table_name = 'commissions'


# tree_commission_details  user Commission details
# tree_commission_bonuses Earnings for each bonus
# tree_general_ledger : Adjustments/Clawback DATA
# tree_commission_runs Commission period details
# tree_commissions we store total earnings and total Clawback for each  period
# tree_bonuses List of all bonuses
def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    commissisions_schema = [
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('tree_user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_user_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField(
            "commissisions",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("currency_code", "STRING"),
                bigquery.SchemaField('earnings', 'INTEGER'),
                bigquery.SchemaField('previous_balance', 'INTEGER'),
                bigquery.SchemaField('balance_forward', 'INTEGER'),
                bigquery.SchemaField('fee', 'INTEGER'),
                bigquery.SchemaField('total', 'INTEGER'),
                bigquery.SchemaField('checksum', 'STRING'),
            ]
        ),
        bigquery.SchemaField(
            "details",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField('source_amount', 'INTEGER'),
                bigquery.SchemaField('percentage', 'INTEGER'),
                bigquery.SchemaField('commission_amount', 'INTEGER'),
                bigquery.SchemaField('level', 'INTEGER'),
                bigquery.SchemaField('paid_level', 'INTEGER')
            ]
        ),
        bigquery.SchemaField(
            "bonuses",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField('period_type', 'STRING'),
                bigquery.SchemaField('description', 'STRING')
            ]
        ),
        bigquery.SchemaField(
            "runs",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField('period', 'STRING'),
                bigquery.SchemaField('period_type', 'STRING'),
                bigquery.SchemaField('description', 'STRING'),
                bigquery.SchemaField('run_status', 'STRING'),
                bigquery.SchemaField('plan', 'STRING'),
            ]
        ),
        bigquery.SchemaField('created_date', 'DATETIME'),
        bigquery.SchemaField('modified_date', 'DATETIME')
    ]
    client.create_table(name=table_name,
                        project=client.client.project,
                        schema=contacts,
                        partition={'type': 'range',
                                   'field': 'client_partition_id',
                                   'start': 1,
                                   'end': 100,
                                   'interval': 1},
                        dataset=dataset,
                        clustering_fields=['leo_eid:STRING', 'ingestion_timestamp:TIMESTAMP'])

    return dataset


def down(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)
    table = client.client.get_table(dataset.table(table_name))
    client.delete_table(table)
