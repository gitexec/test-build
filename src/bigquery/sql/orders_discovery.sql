WITH
##-# users
users AS (
    SELECT
        tree_user_id,
        FORMAT_DATE('%Y', DATE(created)) AS `year`,
        FORMAT_DATE('%m', DATE(created)) AS `month`,
        DATE(created) AS signup
    FROM vibe_staging.users
    WHERE type = 'Market Partner'
    AND icentris_client = 'monat'
),
#-##
##-# orders
orders AS (
SELECT DISTINCT
      u.tree_user_id AS commission_user_id,
      u.year,
      u.month,
      u.signup,
      s.tree_user_id,
      order_id,
      s.type,
      s.is_autoship,
      s.status,
      s.total,
      order_date,
      DATE_DIFF(DATE(s.order_date), signup, DAY) AS days_to_order,
      CASE
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) = 0 THEN '0'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 1 AND 15 THEN '1-15'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 16 AND 90 THEN '16-90'
       # WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 31 AND 90 THEN '31-90'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 91 AND 180 THEN '91-180'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 181 AND 365 THEN '181-365'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 366 AND 730 THEN '366-730'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) > 730 THEN '> 730'
    END AS days_to_order_bucket,
    CASE
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) = 0 THEN '1'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 1 AND 15 THEN '2'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 16 AND 90 THEN '3'
       # WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 31 AND 90 THEN '4'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 91 AND 180 THEN '5'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 181 AND 365 THEN '6'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) BETWEEN 366 AND 730 THEN '7'
        WHEN DATE_DIFF(DATE(s.order_date), signup, DAY) > 730 THEN '8'
    END AS bucket_order
    FROM users u
    LEFT JOIN vibe_staging.orders s
        ON s.commission_user_id = u.tree_user_id
        AND  s.type IN ('Retail', 'Wholesale')
        AND s.status <> 'Cancelled'
        AND s.icentris_client = 'monat'
),
#-##
##-# alpha_omega
alpha_omega AS (
  SELECT
    commission_user_id,
    ARRAY_AGG(IF(type = 'Retail', STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) IGNORE NULLS ORDER BY order_date ASC lIMIT 1)[OFFSET (0)] AS first_retail,
    ARRAY_AGG(IF(type = 'Retail', STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) ORDER BY order_date DESC lIMIT 1)[OFFSET (0)] AS last_retail,
    ARRAY_AGG(IF(type = 'Wholesale', STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) IGNORE NULLS ORDER BY order_date ASC lIMIT 1)[OFFSET (0)] AS first_wholesale,
    ARRAY_AGG(IF(type = 'Wholesale', STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) ORDER BY order_date DESC lIMIT 1)[OFFSET (0)] AS last_wholesale    
  FROM orders
  WHERE days_to_order >= 0
  GROUP BY commission_user_id
)
#-##
SELECT
  user_type,
  order_type,
  is_autoship,
  count,
  sum(count) over (partition by user_type, order_type order by user_type, order_type) as count_over_types,
  round(100 * count / sum(count) over (partition by user_type, order_type order by user_type, order_type), 2) as per_over_types,
  sum(count) over (partition by user_type order by user_type) as count_over_user_type,
  round(100 * count / sum(count) over (partition by user_type order by user_type), 2) as per_over_user_type,  
  sum(count) over () as count_over_all,
  round(100 * count / sum(count) over (), 2) per_over_all
FROM (
SELECT
  u.type as user_type,
  o.type as order_type,
  o.is_autoship,
  COUNT(DISTINCT order_id) AS count,
FROM orders o
JOIN vibe_staging.users u ON o.tree_user_id = u.tree_user_id
GROUP BY u.type, o.type, is_autoship
ORDER BY u.type, o.type, is_autoship
) tmp
