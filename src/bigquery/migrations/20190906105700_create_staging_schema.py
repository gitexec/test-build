from google.cloud import bigquery
from migrations.migration import BigQueryMigration

schemas = {
    'emails': [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("icentris_client", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sender_tree_user_id", "INTEGER"),
        bigquery.SchemaField("message_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sender_id", "INTEGER"),
        bigquery.SchemaField("sender_type", "STRING"),
        bigquery.SchemaField("sent_date", "DATETIME"),
        bigquery.SchemaField("message_to", "STRING"),
        bigquery.SchemaField("from", "STRING"),
        bigquery.SchemaField("subject", "STRING"),
        bigquery.SchemaField("message", "STRING"),
        bigquery.SchemaField("short_message", "STRING"),
        bigquery.SchemaField("draft", "INTEGER"),
        bigquery.SchemaField("delivery_status", "STRING"),
        bigquery.SchemaField("reply_to_id", "INTEGER"),
        bigquery.SchemaField("forwarded_from_id", "INTEGER"),
        bigquery.SchemaField("internal_recipient_count", "INTEGER"),
        bigquery.SchemaField("internal_open_count", "INTEGER"),
        bigquery.SchemaField("size", "INTEGER"),
        bigquery.SchemaField("has_attachments", "INTEGER"),
        bigquery.SchemaField("extras", "STRING"),
        bigquery.SchemaField("created", "DATETIME"),
        bigquery.SchemaField("modified", "DATETIME"),
        bigquery.SchemaField("type", "INTEGER"),
        bigquery.SchemaField("system_generated", "INTEGER"),
        bigquery.SchemaField("tag_deleted", "INTEGER"),
        bigquery.SchemaField("tag_trash", "INTEGER"),
        bigquery.SchemaField("tag_important", "INTEGER"),
        bigquery.SchemaField("tag_starred", "INTEGER"),
        bigquery.SchemaField("cc", "STRING"),
        bigquery.SchemaField("bcc", "STRING"),
        bigquery.SchemaField("undeliverable_count", "INTEGER"),
        bigquery.SchemaField("spam_reported_count", "INTEGER"),
        bigquery.SchemaField("tag_spam", "INTEGER"),
        bigquery.SchemaField(
            "recipients",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("tree_user_id", "INTEGER"),
                bigquery.SchemaField("recipient_id", "INTEGER"),
                bigquery.SchemaField("recipient_type", "STRING"),
                bigquery.SchemaField("consultant_id", "STRING"),
                bigquery.SchemaField("delivery_type", "STRING"),
                bigquery.SchemaField("recipient_to", "STRING"),
                bigquery.SchemaField("subject_interpolated", "STRING"),
                bigquery.SchemaField("message_interpolated", "STRING"),
                bigquery.SchemaField("opened_count", "INTEGER"),
                bigquery.SchemaField("recipient_tag_starred", "INTEGER"),
                bigquery.SchemaField("recipient_tag_important", "INTEGER"),
                bigquery.SchemaField("recipient_tag_spam", "INTEGER"),
                bigquery.SchemaField("recipient_tag_trash", "INTEGER"),
                bigquery.SchemaField("recipient_tag_deleted", "INTEGER"),
                bigquery.SchemaField("from_pyr_user", "INTEGER"),
                bigquery.SchemaField("from_community", "INTEGER"),
                bigquery.SchemaField("from_downline", "INTEGER"),
                bigquery.SchemaField("from_upline", "INTEGER"),
                bigquery.SchemaField("from_team", "INTEGER"),
                bigquery.SchemaField("replied_to_message_id", "INTEGER"),
                bigquery.SchemaField("forwarded_message_id", "INTEGER"),
                bigquery.SchemaField("message_header_id", "STRING"),
                bigquery.SchemaField("recipient_extras", "STRING"),
                bigquery.SchemaField("recipient_created", "DATETIME"),
                bigquery.SchemaField("recipient_modified", "DATETIME"),
                bigquery.SchemaField("read", "DATETIME"),
                bigquery.SchemaField("from_contact", "INTEGER"),
                bigquery.SchemaField("seen", "DATETIME"),
                bigquery.SchemaField("undeliverable", "INTEGER"),
                bigquery.SchemaField("recipient_cc", "INTEGER"),
                bigquery.SchemaField("recipient_bcc", "INTEGER"),
                bigquery.SchemaField("spam_reported", "INTEGER")
            ]
        )
    ],
    'users': [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("icentris_client", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tree_user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "INTEGER"),
        bigquery.SchemaField("sponsor_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("parent_id", "INTEGER"),
        bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField('client_type', 'STRING'),
        bigquery.SchemaField('client_status', 'STRING'),
        bigquery.SchemaField('lifetime_rank', 'STRING', mode="REQUIRED"),
        bigquery.SchemaField('paid_rank', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('client_lifetime_rank', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('client_paid_rank', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("zip", "STRING"),
        bigquery.SchemaField("modified", "DATETIME"),
        bigquery.SchemaField("created", "DATETIME"),
        bigquery.SchemaField("coded", "DATETIME")
    ],
    'orders': [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("icentris_client", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tree_user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField('client_type', 'STRING'),
        bigquery.SchemaField('client_status', 'STRING'),
        bigquery.SchemaField("is_autoship", "BOOLEAN"),
        bigquery.SchemaField("order_date", "DATETIME"),
        bigquery.SchemaField("currency_code", "STRING"),
        bigquery.SchemaField("total", "NUMERIC"),
        bigquery.SchemaField("sub_total", "NUMERIC"),
        bigquery.SchemaField("tax_total", "NUMERIC"),
        bigquery.SchemaField("shipping_total", "NUMERIC"),
        bigquery.SchemaField("bv_total", "NUMERIC"),
        bigquery.SchemaField("cv_total", "NUMERIC"),
        bigquery.SchemaField("discount_total", "NUMERIC"),
        bigquery.SchemaField("discount_code", "STRING"),
        bigquery.SchemaField("timezone", "STRING"),
        bigquery.SchemaField("shipped_date", "DATETIME"),
        bigquery.SchemaField("shipping_city", "STRING"),
        bigquery.SchemaField("shipping_state", "STRING"),
        bigquery.SchemaField("shipping_zip", "STRING"),
        bigquery.SchemaField("shipping_country", "STRING"),
        bigquery.SchemaField("shipping_county", "STRING"),
        bigquery.SchemaField("tracking_number", "STRING"),
        bigquery.SchemaField("created", "DATETIME"),
        bigquery.SchemaField("modified", "DATETIME"),
        bigquery.SchemaField("client_order_id", "INTEGER"),
        bigquery.SchemaField("client_user_id", "STRING"),
        bigquery.SchemaField(
            "products",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("product_id", "INTEGER"),
                bigquery.SchemaField("product_code", "STRING"),
                bigquery.SchemaField("product_description", "STRING"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("price_each", "NUMERIC"),
                bigquery.SchemaField("price_total", "NUMERIC"),
                bigquery.SchemaField("weight_each", "NUMERIC"),
                bigquery.SchemaField("weight_total", "NUMERIC"),
                bigquery.SchemaField("tax", "NUMERIC"),
                bigquery.SchemaField("bv", "NUMERIC"),
                bigquery.SchemaField("bv_each", "NUMERIC"),
                bigquery.SchemaField("cv_each", "NUMERIC"),
                bigquery.SchemaField("cv", "NUMERIC"),
                bigquery.SchemaField("parent_product_id", "INTEGER"),
                bigquery.SchemaField("item_tracking_number", "STRING"),
                bigquery.SchemaField("item_shipping_city", "STRING"),
                bigquery.SchemaField("item_shipping_state", "STRING"),
                bigquery.SchemaField("item_shipping_zip", "STRING"),
                bigquery.SchemaField("item_shipping_county", "STRING"),
                bigquery.SchemaField("item_shipping_country", "STRING")
            ]
        )
    ],
    'tasks': [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField("icentris_client", "STRING", mode="REQUIRED"),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("task_title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("due_date", "DATETIME"),
        bigquery.SchemaField("user_id", "INTEGER"),
        bigquery.SchemaField("tree_user_id", "INTEGER"),
        bigquery.SchemaField("from_system", "BOOLEAN"),
        bigquery.SchemaField("priority", "INTEGER"),
        bigquery.SchemaField("created", "DATETIME"),
        bigquery.SchemaField("completed", "DATETIME")
    ],
    'sites': [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField("icentris_client", "STRING", mode="REQUIRED"),
        bigquery.SchemaField('client_wrench_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("site_id", "INTEGER"),
        bigquery.SchemaField("user_id", "INTEGER"),
        bigquery.SchemaField("tree_user_id", "INTEGER"),
        bigquery.SchemaField(
            "analytics_tracking_codes",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("third_party_tracking_company", "STRING"),
                bigquery.SchemaField("tracking_code", "STRING"),
            ]
        ),
        bigquery.SchemaField(
            "visitors",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("visitor_id", "STRING"),
                bigquery.SchemaField("last_visit_date", "DATETIME"),
                bigquery.SchemaField("visit_count", "INTEGER"),
                bigquery.SchemaField("ipaddress", "STRING"),
                bigquery.SchemaField("browser_agent", "STRING"),
                bigquery.SchemaField("created", "DATETIME"),
            ]
        )
    ],
}


def up(client):
    client = BigQueryMigration(client)

    dataset = client.create_dataset('staging')
    for tbl, schema in schemas.items():
        clusters = ['leo_eid:STRING',  'ingestion_timestamp:TIMESTAMP']

        client.create_table(name=tbl,
                            project=client.client.project, schema=schema, dataset=dataset,
                            partition={
                                'type': 'range',
                                'field':
                                'client_partition_id',
                                'start': 1,
                                'end': 100,
                                'interval': 1},
                            clustering_fields=clusters)
    return dataset


def down(client):
    migration = BigQueryMigration(client)
    migration.delete_dataset('staging')
