from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'warehouse'
table_name = 'distributor_orders'

schemas = {
    'distributor_orders': [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('tree_user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('days_to_first_wholesale_order', 'INTEGER'),
        bigquery.SchemaField('days_to_first_autoship_order', 'INTEGER'),
        bigquery.SchemaField('days_to_first_retail_order', 'INTEGER'),
        bigquery.SchemaField('days_to_last_wholesale_order', 'INTEGER'),
        bigquery.SchemaField('days_to_last_autoship_order', 'INTEGER'),
        bigquery.SchemaField('days_to_last_retail_order', 'INTEGER'),
        bigquery.SchemaField('days_since_last_wholesale_order', 'INTEGER'),
        bigquery.SchemaField('days_since_last_autoship_order', 'INTEGER'),
        bigquery.SchemaField('days_since_last_retail_order', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_avg_wholesale_amount', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_avg_autoship_amount', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_avg_retail_amount', 'NUMERIC'),
        bigquery.SchemaField('lifetime_avg_wholesale_amount', 'NUMERIC'),
        bigquery.SchemaField('lifetime_avg_autoship_amount', 'NUMERIC'),
        bigquery.SchemaField('lifetime_avg_retail_amount', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_total_wholesale_amount', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_total_autoship_amount', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_total_retail_amount', 'NUMERIC'),
        bigquery.SchemaField('lifetime_total_wholesale_amount', 'NUMERIC'),
        bigquery.SchemaField('lifetime_total_autoship_amount', 'NUMERIC'),
        bigquery.SchemaField('lifetime_total_retail_amount', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_total_wholesale_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_total_autoship_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_total_retail_count', 'INTEGER'),
        bigquery.SchemaField('lifetime_total_wholesale_count', 'INTEGER'),
        bigquery.SchemaField('lifetime_total_autoship_count', 'INTEGER'),
        bigquery.SchemaField('lifetime_total_retail_count', 'INTEGER')
    ],
    'distributor_downlines': [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('tree_user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('days_to_first_downline_distributor', 'INTEGER'),
        bigquery.SchemaField('downline_distributor_signups_last_30', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_gen1_distributors_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_gen2_distributors_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_gen3_distributors_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_gen4_distributors_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_gen5_distributors_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_gen6_distributors_count', 'INTEGER'),
        bigquery.SchemaField('gen1_distributors_count', 'INTEGER'),
        bigquery.SchemaField('gen2_distributors_count', 'INTEGER'),
        bigquery.SchemaField('gen3_distributors_count', 'INTEGER'),
        bigquery.SchemaField('gen4_distributors_count', 'INTEGER'),
        bigquery.SchemaField('gen5_distributors_count', 'INTEGER'),
        bigquery.SchemaField('gen6_distributors_count', 'INTEGER'),
        bigquery.SchemaField('rolling_12_month_gen1_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_gen2_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_gen3_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_gen4_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_gen5_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('rolling_12_month_gen6_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('gen1_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('gen2_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('gen3_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('gen4_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('gen5_distributors_lcv', 'NUMERIC'),
        bigquery.SchemaField('gen6_distributors_lcv', 'NUMERIC'),
    ]
}


def up(client):
    migration = BigQueryMigration(client)

    dataset = migration.create_dataset(dataset_name)  # use me if you are creating a new dataset. -- ndg 2/5/20

    for tbl, schema in schemas.items():
        migration.create_table(name=tbl,
                               project=migration.client.project, schema=schema, dataset=dataset,
                               partition={
                                   'type': 'range',
                                   'field':
                                   'client_partition_id',
                                   'start': 1,
                                   'end': 100,
                                   'interval': 1},
                               clustering_fields=migration.default_clustering_fields)
    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)

    tbls = migration.client.list_tables(dataset)

    for item in tbls:
        migration.client.delete_table(item)

    migration.client.delete_dataset(dataset)
