## monat
CASE
      WHEN t.description = 'B' THEN 'Backorder Cancelled'
      WHEN t.description = 'C' THEN 'Credit/RMA'
      WHEN t.description = 'D' THEN 'Retail'
      WHEN t.description = 'E' THEN 'Exchange/RMA'
      WHEN t.description = 'I' THEN 'Wholesale'
      WHEN t.description = 'R' THEN 'Replacement'
      WHEN t.description = 'X' THEN 'Exchange/Order'
      WHEN t.description = 'Y' THEN 'Point Redemption'
  END

## wv
UPDATE vibe_staging.orders
  SET status = CASE
    WHEN client_status IN ('ACH Declined', 'Cancelled', 'CC Declined') THEN 'Inactive'
    ELSE 'Active'
  END
WHERE client_partition_id = 2

## nsp
UPDATE vibe_staging.orders o
  SET commission_user_id = CASE
      WHEN u.type <> 'Distributor' THEN u.sponsor_id
      WHEN u.type = 'Distributor' AND u.coded > o.order_date THEN u.sponsor_id
      WHEN u.type = 'Distributor' THEN u.tree_user_id
  END,
  type = CASE
    WHEN u.type = 'Distributor' AND u.coded <= o.order_date THEN 'Wholesale'
    WHEN u.type = 'Distributor' AND u.coded > o.order_date THEN 'Autoship'
    WHEN u.type = 'Customer' THEN 'Retail'
    WHEN u.type = 'Autoship' THEN 'Autoship'
  END
FROM vibe_staging.users u
WHERE o.icentris_client = 'naturessunshine'
AND o.client_partition_id = u.client_partition_id
AND o.tree_user_id = u.tree_user_id

