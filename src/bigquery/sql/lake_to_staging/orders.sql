
INSERT INTO vibe_staging.orders (
  icentris_client,
  client_partition_id,
  client_wrench_id,
  leo_eid,
  ingestion_timestamp,
  order_id,
  commission_user_id,
  tree_user_id,
  type,
  status,
  client_type,
  client_status,
  is_autoship,
  order_date,
  currency_code,
  total,
  sub_total,
  tax_total,
  shipping_total,
  bv_total,
  cv_total,
  discount_total,
  discount_code,
  timezone,
  shipped_date,
  shipping_city,
  shipping_state,
  shipping_zip,
  shipping_country,
  shipping_county,
  tracking_number,
  created,
  modified,
  client_order_id,
  client_user_id,
  products
)
SELECT
  c.icentris_client,
  c.partition_id,
  c.wrench_id,
  'z/0',
  CURRENT_TIMESTAMP(),
  o.id,
  CASE
      WHEN u.type <> 'Distributor' THEN u.sponsor_id
      WHEN u.type = 'Distributor' AND u.coded > o.order_date THEN u.sponsor_id
      WHEN u.type = 'Distributor' THEN u.tree_user_id
  END AS commission_user_id,
  o.tree_user_id,
  CASE
    WHEN u.type = 'Distributor' AND u.coded <= o.order_date THEN 'Wholesale'
    WHEN u.type = 'Distributor' AND u.coded > o.order_date THEN 'Autoship'
    WHEN u.type = 'Customer' THEN 'Retail'
    WHEN u.type = 'Autoship' THEN 'Autoship'
  END AS `type`,
  'tbd' AS status,
  t.description AS client_type,
  s.description AS `client_status`,
  IF(o.`autoship_template_id` > 0, True, False) AS is_autoship,
  o.`order_date`,
  o.`currency_code`,
  CAST(o.`total` AS NUMERIC),
  CAST(o.`sub_total` AS NUMERIC),
  CAST(o.`tax_total` AS NUMERIC),
  CAST(o.`shipping_total` AS NUMERIC),
  CAST(o.`bv_total` AS NUMERIC),
  CAST(o.`cv_total` AS NUMERIC),
  CAST(o.`discount_total` AS NUMERIC),
  o.`discount_code`,
  o.`timezone`,
  o.`shipped_date`,
  o.`shipping_city`,
  o.`shipping_state`,
  o.`shipping_zip`,
  o.`shipping_country`,
  o.`shipping_county`,
  o.`tracking_number`,
  o.`created_date`,
  o.`modified_date`,
  o.`client_order_id`,
  o.`client_user_id`,
  ARRAY_AGG( STRUCT (
    i.`product_id`,
    i.`product_code`,
    i.`product_description`,
    i.`quantity`,
    CAST(i.`price_each` AS NUMERIC),
    CAST(i.`price_total` AS NUMERIC),
    CAST(i.`weight_each` AS NUMERIC),
    CAST(i.`weight_total` AS NUMERIC),
    CAST(i.`tax` AS NUMERIC),
    CAST(i.`bv` AS NUMERIC),
    CAST(i.`bv_each` AS NUMERIC),
    CAST(i.`cv_each` AS NUMERIC),
    CAST(i.`cv` AS NUMERIC),
    i.`parent_product_id`,
    i.`tracking_number`,
    i.`shipping_city`,
    i.`shipping_state`,
    i.`shipping_zip`,
    i.`shipping_county`,
    i.`shipping_country`
  )) AS items
FROM pyr_worldventures_prd.tree_orders o
INNER JOIN vibe_staging.users u ON o.tree_user_id = u.tree_user_id
LEFT OUTER JOIN pyr_worldventures_prd.tree_order_items i ON o.id = i.order_id
INNER JOIN pyr_worldventures_prd.tree_order_types t ON o.order_type_id = t.id
INNER JOIN pyr_worldventures_prd.tree_order_statuses s ON o.order_status_id = s.id
INNER JOIN system.clients c
    ON c.icentris_client = 'worldventures'
    AND u.client_partition_id = c.partition_id
GROUP BY
  c.icentris_client,
  c.partition_id,
  c.wrench_id,
  o.id,
  o.tree_user_id,
  u.type,
  u.tree_user_id,
  u.sponsor_id,
  commission_user_id,
  `type`,
  `status`,
  client_type,
  client_status,
  is_autoship,
  o.`order_date`,
  o.`currency_code`,
  o.`total`,
  o.`sub_total`,
  o.`tax_total`,
  o.`shipping_total`,
  o.`bv_total`,
  o.`cv_total`,
  o.`discount_total`,
  o.`discount_code`,
  o.`timezone`,
  o.`shipped_date`,
  o.`shipping_city`,
  o.`shipping_state`,
  o.`shipping_zip`,
  o.`shipping_country`,
  o.`shipping_county`,
  o.`tracking_number`,
  o.`created_date`,
  o.`modified_date`,
  o.`client_order_id`,
  o.`client_user_id`

