
##-# ids
WITH ids AS (
        SELECT icentris_client, id AS contact_id, 'contacts' AS tbl
        FROM lake.pyr_contacts
        WHERE ingestion_timestamp BETWEEN "{first_ingestion_timestamp}" AND "{last_ingestion_timestamp}"
    UNION DISTINCT
        SELECT icentris_client, contact_id, 'emails'
        FROM lake.pyr_contact_emails
        WHERE ingestion_timestamp BETWEEN "{first_ingestion_timestamp}" AND "{last_ingestion_timestamp}"
    UNION DISTINCT
        SELECT icentris_client, id AS contact_id, 'phone_numbers' AS tbl
        FROM lake.pyr_contact_phone_numbers
        WHERE ingestion_timestamp BETWEEN "{first_ingestion_timestamp}" AND "{last_ingestion_timestamp}"
    UNION DISTINCT
        SELECT icentris_client, id AS contact_id, 'categories' AS tbl
        FROM lake.pyr_contacts_contact_categories
        WHERE ingestion_timestamp BETWEEN "{first_ingestion_timestamp}" AND "{last_ingestion_timestamp}"
),
#-##
##-# contacts
contacts AS (
  SELECT
    pc.icentris_client,
    pc.id,
    pc.user_id,
    pc.tree_user_id,
    pc.contact_type,
    pc.level_of_interest,
    pc.first_name,
    pc.last_name,
    pc.birthday,
    pc.created_at,
    pc.updated_at,
    pc.is_downline_contact,
    pc.opt_in,
    pc.info,
    pc.avatar_file_name,
    pc.avatar_content_type,
    pc.avatar_file_size,
    pc.avatar_updated_at,
    pc.leo_eid,
    ARRAY_AGG(STRUCT(pc.address1, pc.address2, pc.city, pc.postal_code, pc.state, pc.country)) as addresses
  FROM (
  SELECT
    c.*,
    ROW_NUMBER() OVER (PARTITION BY c.icentris_client, c.id ORDER BY c.leo_eid DESC, c.ingestion_timestamp DESC) AS rn
  FROM ids
  JOIN lake.pyr_contacts c ON ids.icentris_client = c.icentris_client AND ids.contact_id = c.id
  ) pc
  WHERE rn = 1
  GROUP BY
    pc.icentris_client,
    pc.id,
    pc.user_id,
    pc.tree_user_id,
  pc.contact_type,
  pc.level_of_interest,
  pc.first_name,
  pc.last_name,
  pc.birthday,
  pc.created_at,
  pc.updated_at,
  pc.is_downline_contact,
  pc.opt_in,
  pc.info,
  pc.avatar_file_name,
  pc.avatar_content_type,
  pc.avatar_file_size,
  pc.avatar_updated_at,
  pc.leo_eid

),
#-##
##-# tree_users
tree_users AS (
  SELECT
    c.icentris_client,
    con.id AS contact_id,
    c.id,
    c.sponsor_id,
    ROW_NUMBER() OVER (PARTITION BY con.icentris_client, con.id
        ORDER BY c.leo_eid DESC, c.ingestion_timestamp DESC) AS rn
  FROM lake.tree_users c
  JOIN contacts con ON c.icentris_client = con.icentris_client AND c.id = con.tree_user_id
),
#-##
##-# sponsor_users
sponsor_users AS (
  SELECT
    c.icentris_client,
    con.id AS contact_id,
    c.id,
    c.tree_user_id,
    ROW_NUMBER() OVER (PARTITION BY con.icentris_client, con.id
        ORDER BY c.leo_eid DESC, c.ingestion_timestamp DESC) AS rn
  FROM lake.users c
  JOIN contacts con ON c.icentris_client = con.icentris_client AND c.id = con.user_id
),
#-##
##-# emails
emails AS (
  SELECT
    icentris_client,
    contact_id,
    ARRAY_AGG(STRUCT(e.email, e.preferred, e.undeliverable_count, e.spam_reported_count, e.source)) AS emails
  FROM (
    SELECT c.*,
      ROW_NUMBER() OVER (PARTITION BY c.icentris_client, c.contact_id, c.email
          ORDER BY c.leo_eid DESC, c.ingestion_timestamp DESC) AS rn
    FROM ids
    LEFT JOIN lake.pyr_contact_emails c ON ids.icentris_client = c.icentris_client AND ids.contact_id = c.contact_id
  ) e
  WHERE rn = 1
  GROUP BY icentris_client, contact_id
),
#-##
##-# phones
phones AS (
  SELECT
    icentris_client,
    contact_id,
    ARRAY_AGG(STRUCT(pn.phone_number, pn.label, pn.unformatted_phone_number, pn.source, pn.dialing_code)) as phone_numbers
  FROM (
  SELECT c.*,
    ROW_NUMBER() OVER (PARTITION BY c.icentris_client, c.contact_id, c.unformatted_phone_number
        ORDER BY c.leo_eid DESC, c.ingestion_timestamp DESC) AS rn
  FROM ids
  LEFT JOIN lake.pyr_contact_phone_numbers c ON ids.icentris_client = c.icentris_client AND ids.contact_id = c.contact_id
  ) pn
  WHERE rn = 1
  GROUP BY icentris_client, contact_id
),
#-##
##-# categories
categories AS (
  SELECT
    icentris_client,
    contact_id,
    ARRAY_AGG(STRUCT(cc.category, cc.created_at, cc.updated_at)) AS categories
  FROM (
    SELECT c.*,
        cc.category_name AS category,
        ROW_NUMBER() OVER (PARTITION BY c.icentris_client, c.contact_id
            ORDER BY c.leo_eid DESC, c.ingestion_timestamp DESC) AS rn
    FROM ids
    LEFT JOIN lake.pyr_contacts_contact_categories c ON ids.icentris_client = c.icentris_client AND ids.contact_id = c.contact_id
    JOIN (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id, icentris_client ORDER BY leo_eid ASC, ingestion_timestamp DESC) AS rn
        FROM lake.pyr_contact_categories
    ) cc ON c.contact_category_id = cc.id AND c.icentris_client = cc.icentris_client AND cc.rn = 1
  ) cc
  WHERE rn = 1
  GROUP BY icentris_client, contact_id
)
#-##
##-# SELECT
SELECT
  c.partition_id AS client_partition_id,
  c.wrench_id AS client_wrench_id,
  c.icentris_client,
  pc.id,
  IF(tu.id IS NOT NULL, tu.sponsor_id, spu.tree_user_id) owner_tree_user_id,
  IF(tu.id IS NOT NULL, spu.id, pc.user_id) AS owner_user_id,
  IF(tu.id IS NOT NULL, tu.id, NULL) AS tree_user_id,
  IF(tu.id IS NOT NULL, pc.user_id, NULL) AS user_id,
  pc.contact_type AS type,
  pc.level_of_interest,
  pc.first_name,
  pc.last_name,
  pc.birthday,
  pc.created_at,
  pc.updated_at,
  pc.is_downline_contact AS is_downline,
  pc.opt_in,
  pc.info,
  pc.avatar_file_name,
  pc.avatar_content_type,
  pc.avatar_file_size,
  pc.avatar_updated_at,
  pc.leo_eid,
  pc.addresses,
  e.emails,
  pn.phone_numbers,
  cc.categories,
#-##
##-# FROM
FROM contacts pc
INNER JOIN system.clients c ON pc.icentris_client = c.icentris_client
LEFT JOIN tree_users tu ON pc.icentris_client = tu.icentris_client AND pc.id = tu.contact_id AND tu.rn = 1
LEFT JOIN sponsor_users spu ON pc.icentris_client = spu.icentris_client AND pc.id = spu.contact_id AND spu.rn = 1
JOIN emails e ON pc.icentris_client = e.icentris_client AND pc.id = e.contact_id
JOIN phones pn ON pc.icentris_client = pn.icentris_client AND pc.id = pn.contact_id
JOIN categories cc ON c.icentris_client = cc.icentris_client AND pc.id = cc.contact_id
#-##
