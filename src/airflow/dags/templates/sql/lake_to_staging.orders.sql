##-# ids
WITH ids AS (
  SELECT icentris_client, id AS order_id, 'orders' AS tbl
  FROM lake.tree_orders
  WHERE ingestion_timestamp BETWEEN '{first_ingestion_timestamp}' AND '{last_ingestion_timestamp}'
  UNION DISTINCT
  SELECT icentris_client, id AS order_id, 'order_items' AS tbl
  FROM lake.tree_order_items
  WHERE ingestion_timestamp BETWEEN '{first_ingestion_timestamp}' AND '{last_ingestion_timestamp}'
),
#-##
##-# tree_order_items
tree_order_items AS (
    SELECT
        toi.icentris_client,
        toi.order_id,
        ARRAY_AGG(STRUCT(
            toi.product_id,
            toi.product_code,
            toi.product_description,
            toi.quantity,
            CAST(toi.price_each AS NUMERIC) AS price_each,
            CAST(toi.price_total AS NUMERIC) AS price_total,
            CAST(toi.weight_each AS NUMERIC) AS weight_each,
            CAST(toi.weight_total AS NUMERIC) AS weight_total,
            CAST(toi.tax AS NUMERIC) AS tax,
            CAST(toi.bv AS NUMERIC) AS bv,
            CAST(toi.bv_each AS NUMERIC) AS bv_each,
            CAST(toi.cv_each AS NUMERIC) AS cv_each,
            CAST(toi.cv AS NUMERIC) AS cv,
            toi.parent_product_id,
            toi.tracking_number AS item_tracking_number,
            toi.shipping_city AS item_shipping_city,
            toi.shipping_state AS item_shipping_state,
            toi.shipping_zip AS item_shipping_zip,
            toi.shipping_county AS item_shipping_county,
            toi.shipping_country AS item_shipping_country
        )) AS products
    FROM (
        SELECT
            toi.*,
            ROW_NUMBER() OVER (PARTITION BY toi.icentris_client, toi.id
                ORDER BY toi.leo_eid DESC, toi.ingestion_timestamp DESC) AS rn
        FROM lake.tree_order_items toi
        JOIN ids o ON toi.icentris_client = o.icentris_client AND toi.order_id = o.order_id
    ) toi
    WHERE rn = 1
    GROUP BY toi.icentris_client, toi.order_id
),
#-##
##-# orders
orders AS (
    SELECT
        o.icentris_client,
        o.id,
        o.tree_user_id,
        o.leo_eid,
        o.ingestion_timestamp,
        o.autoship_template_id,
        o.order_date,
        o.currency_code,
        o.total,
        o.sub_total,
        o.tax_total,
        o.shipping_total,
        o.bv_total,
        o.cv_total,
        o.discount_total,
        o.discount_code,
        o.timezone,
        o.shipped_date,
        o.shipping_city,
        o.shipping_state,
        o.shipping_zip,
        o.shipping_country,
        o.shipping_county,
        o.tracking_number,
        o.created_date,
        o.modified_date,
        o.client_order_id,
        o.client_user_id,
        o.order_status_id,
        o.order_type_id,
        ROW_NUMBER() OVER (PARTITION BY o.icentris_client, o.id ORDER BY o.leo_eid DESC, o.ingestion_timestamp DESC) AS rn
    FROM ids
    JOIN lake.tree_orders o ON ids.icentris_client = o.icentris_client AND ids.order_id = o.id
),
#-##
##-# tree_users
tree_users AS (
    SELECT
        tu.icentris_client,
        tu.id,
        tu.sponsor_id,
        tu.user_type_id,
        ROW_NUMBER() OVER (PARTITION BY tu.icentris_client, tu.id
            ORDER BY tu.leo_eid DESC, tu.ingestion_timestamp DESC) AS rn
    FROM orders o
    LEFT JOIN lake.tree_users tu ON tu.icentris_client = o.icentris_client AND tu.id = o.tree_user_id
),
#-##
##-# tree_user_types
tree_user_types AS (
  SELECT
    tut.icentris_client,
    tut.id,
    tut.description,
    ROW_NUMBER() OVER (PARTITION BY tut.icentris_client, tut.id
        ORDER BY tut.leo_eid DESC, tut.ingestion_timestamp DESC) AS rn
    FROM lake.tree_user_types tut
),
#-##
##-# tree_order_statuses
tree_order_statuses AS (
  SELECT
    tos.icentris_client,
    tos.id,
    tos.description,
    ROW_NUMBER() OVER (PARTITION BY tos.icentris_client, tos.id
        ORDER BY tos.leo_eid DESC, tos.ingestion_timestamp DESC) AS rn
    FROM lake.tree_order_statuses tos
),
#-##
##-# tree_order_types
tree_order_types AS (
  SELECT
    tot.icentris_client,
    tot.id,
    tot.description,
    ROW_NUMBER() OVER (PARTITION BY tot.icentris_client, tot.id
        ORDER BY tot.leo_eid DESC, tot.ingestion_timestamp DESC) AS rn
    FROM lake.tree_order_types tot
)
#-##
##-# SELECT
SELECT
    c.icentris_client,
    c.partition_id AS client_partition_id,
    c.wrench_id AS client_wrench_id,
    tu.id AS tree_user_id,
    tu.sponsor_id,
    tut.description AS client_user_type,
    tot.description AS client_type,
    tos.description AS client_status,
    o.id as order_id,
    IF(o.autoship_template_id > 0, True, False) AS is_autoship,
    o.order_date,
    o.currency_code,
    CAST(o.total AS NUMERIC) AS total,
    CAST(o.sub_total AS NUMERIC) AS sub_total,
    CAST(o.tax_total AS NUMERIC) AS tax_total,
    CAST(o.shipping_total AS NUMERIC) AS shipping_total,
    CAST(o.bv_total AS NUMERIC) AS bv_total,
    CAST(o.cv_total AS NUMERIC) AS cv_total,
    CAST(o.discount_total AS NUMERIC) AS discount_total,
    o.discount_code,
    o.timezone,
    o.shipped_date,
    o.shipping_city,
    o.shipping_state,
    o.shipping_zip,
    o.shipping_country,
    o.shipping_county,
    o.tracking_number,
    o.created_date as created,
    o.modified_date as modified,
    o.client_order_id,
    o.client_user_id,
    o.leo_eid,
    o.ingestion_timestamp,
    toi.products
#-##
##-# FROM
FROM
  orders o
  INNER JOIN system.clients c ON o.icentris_client = c.icentris_client
  LEFT JOIN tree_order_items toi
    ON o.icentris_client = toi.icentris_client AND o.id = toi.order_id
  LEFT JOIN tree_users tu
    ON o.icentris_client = tu.icentris_client AND o.tree_user_id = tu.id AND tu.rn = 1
  LEFT JOIN tree_order_statuses tos
    ON o.icentris_client = tos.icentris_client AND o.order_status_id = tos.id AND tos.rn = 1
  LEFT JOIN tree_user_types tut
    ON tu.icentris_client = tut.icentris_client AND tu.user_type_id = tut.id AND tut.rn = 1
  LEFT JOIN tree_order_types tot
    ON tu.icentris_client = tot.icentris_client AND o.order_type_id = tot.id AND tot.rn = 1
#-##
WHERE o.rn = 1
