##-# ids
WITH ids AS (
  SELECT icentris_client, id AS tree_user_id, 'tree_users' AS tbl
  FROM lake.tree_users
  WHERE ingestion_timestamp BETWEEN "{first_ingestion_timestamp}" AND "{last_ingestion_timestamp}"
  UNION DISTINCT
  SELECT icentris_client, tree_user_id, 'users' AS tbl
  FROM lake.users
  WHERE ingestion_timestamp BETWEEN "{first_ingestion_timestamp}" AND "{last_ingestion_timestamp}"
),
#-##
##-# tree_users
tree_users AS (
    SELECT
        tu.icentris_client,
        tu.leo_eid,
        tu.ingestion_timestamp,
        tu.id,
        tu.user_type_id,
        tu.user_status_id,
        tu.rank_id,
        tu.paid_rank_id,
        tu.sponsor_id,
        tu.parent_id,
        tu.zip,
        tu.created_date,
        tu.updated_date,
        ROW_NUMBER() OVER (PARTITION BY tu.icentris_client, tu.id ORDER BY tu.leo_eid DESC, tu.ingestion_timestamp DESC) AS rn
    FROM
        lake.tree_users tu
        JOIN ids ON tu.icentris_client = ids.icentris_client AND tu.id = ids.tree_user_id
),
#-##
##-# users
users AS (
    SELECT
      u.icentris_client,
      u.tree_user_id,
      u.id,
      ROW_NUMBER() OVER (PARTITION BY u.icentris_client, u.tree_user_id ORDER BY u.leo_eid DESC, u.ingestion_timestamp DESC) AS rn
    FROM
      lake.users u
      JOIN ids ON u.icentris_client = ids.icentris_client AND u.tree_user_id = ids.tree_user_id
),
#-##
##-# tree_user_types
tree_user_types AS (
    SELECT
      tut.*,
      ROW_NUMBER() OVER (PARTITION BY tut.icentris_client, tut.id ORDER BY tut.leo_eid DESC, tut.ingestion_timestamp DESC) AS rn
    FROM
      lake.tree_user_types tut
),
#-##
##-# tree_user_types
tree_user_statuses AS (
    SELECT
      tus.*,
      ROW_NUMBER() OVER (PARTITION BY tus.icentris_client, tus.id ORDER BY tus.leo_eid DESC, tus.ingestion_timestamp DESC) AS rn
    FROM
      lake.tree_user_statuses tus
),
#-##
##-# ranks
ranks AS (
    SELECT
      prd.*,
      ROW_NUMBER() OVER (PARTITION BY prd.icentris_client, prd.id ORDER BY prd.leo_eid DESC, prd.ingestion_timestamp DESC) AS rn
    FROM
      lake.pyr_rank_definitions prd
),
##-# paid_ranks
paid_ranks AS (
    SELECT
      prd.*,
      ROW_NUMBER() OVER (PARTITION BY prd.icentris_client, prd.id ORDER BY prd.leo_eid DESC, prd.ingestion_timestamp DESC) AS rn
    FROM
      lake.pyr_rank_definitions prd
)
SELECT
    c.partition_id AS client_partition_id,
    c.wrench_id AS client_wrench_id,
    c.icentris_client,
    tu.leo_eid,
    tu.ingestion_timestamp,
    tu.id AS tree_user_id,
    u.id AS user_id,
    tu.sponsor_id,
    tu.parent_id,
    tu.zip,
    tu.created_date AS created,
    tu.updated_date AS modified,
    tut.description AS client_type,
    tus.description AS client_status,
    '' AS paid_rank,
    NULL AS paid_rank_level,
    pr.name AS client_paid_rank,
    pr.level AS client_paid_rank_level,
    '' AS lifetime_rank,
    NULL AS lifetime_rank_level,
    r.name AS client_lifetime_rank,
    r.level AS client_lifetime_rank_level
#-##
##-# FROM
FROM
  tree_users tu
  INNER JOIN system.clients c ON tu.icentris_client = c.icentris_client
  LEFT JOIN users u ON tu.icentris_client = u.icentris_client AND tu.id = u.tree_user_id AND u.rn = 1
  LEFT JOIN tree_user_types tut ON tu.icentris_client = tut.icentris_client AND tu.user_type_id = tut.id AND tut.rn = 1
  LEFT JOIN tree_user_statuses tus ON tu.icentris_client = tus.icentris_client AND tu.user_status_id = tus.id AND tus.rn = 1
  LEFT JOIN paid_ranks pr ON tu.icentris_client = pr.icentris_client AND tu.paid_rank_id = pr.level AND pr.rn = 1
  LEFT JOIN ranks r ON tu.icentris_client = r.icentris_client AND tu.rank_id = r.level AND r.rn = 1
#-##
WHERE tu.rn = 1 AND tu.sponsor_id IS NOT NULL AND tu.user_type_id IS NOT NULL AND tu.rank_id IS NOT NULL
