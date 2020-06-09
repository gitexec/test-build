INSERT INTO pii.users (
    client_partition_id,
    client_wrench_id,
    icentris_client,
    leo_eid,
    ingestion_timestamp,
    tree_user_id,
    first_name,
    last_name,
    company_name,
    street,
    city,
    state,
    country,
    email,
    phone,
    mobile_phone,
    gender,
    birth_date
)
SELECT
  c.partition_id,
  c.wrench_id,
  c.icentris_client,
  'z/0',
  CURRENT_TIMESTAMP(),
  tu.id,
  first_name,
  last_name,
  company_name,
  CONCAT(address1,
IF(address2 IS NOT NULL AND address2 <> '', ' ', ''),
IF(address2 IS NOT NULL AND address2 <> '', address2, '')),
  city,
  state,
  country,
  email,
  phone,
  mobile_phone,
  CASE
    WHEN gender IN ('F',
'female') THEN 'female'
    WHEN gender IN ('M',
'male') THEN 'male'
    ELSE 'none'
  END AS gender,
  birth_date
FROM pyr_worldventures_prd.tree_users tu
INNER JOIN system.clients c ON c.icentris_client = 'worldventures'
