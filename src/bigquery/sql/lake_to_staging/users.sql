INSERT INTO vibe_staging.users (
    client_partition_id,
    client_wrench_id,
    icentris_client,
    leo_eid,
    ingestion_timestamp,
    tree_user_id,
    user_id,
    sponsor_id,
    parent_id,
    type,
    status,
    client_type,
    zip,
    coded,
    created_date,
    client_status,
    modified)
SELECT
  c.partition_id,
  c.wrench_id,
  c.icentris_client,
  'z/0',
  CURRENT_TIMESTAMP(),
  tu.id AS `tree_user_id`,
  u.id AS `user_id`,
  tu.sponsor_id,
  tu.parent_id,
  'tbd',
  'tbd',
  tt.description,
  tu.zip as zip,
  tu.created_date,
  tus.description,
  tu.updated_date
FROM pyr_worldventures_prd.tree_users tu
LEFT OUTER JOIN pyr_worldventures_prd.users u ON tu.id = u.tree_user_id
INNER JOIN pyr_worldventures_prd.tree_user_types tt ON tu.user_type_id = tt.id
INNER JOIN pyr_worldventures_prd.tree_user_statuses tus ON tu.user_status_id = tus.id
INNER JOIN system.clients c ON c.icentris_client = 'worldventures'
