WITH
##-# users
users AS (
    SELECT
        tree_user_id,
        FORMAT_DATE('%Y', DATE(created)) AS `year`,
        FORMAT_DATE('%m', DATE(created)) AS `month`,
        DATE(created) AS signup
    FROM vibe_staging.users
    #WHERE created BETWEEN '2018-07-01' AND '2019-06-30'
    WHERE type = 'Distributor'
    AND client_partition_id = 3
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
        AND s.status <> 'Cancelled'
),
#-##
##-# alpha_omega
alpha_omega AS (
  SELECT
    commission_user_id,
    ARRAY_AGG(STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order) ORDER BY order_date ASC lIMIT 1)[OFFSET (0)] AS first,
    ARRAY_AGG(STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order) ORDER BY order_date DESC lIMIT 1)[OFFSET (0)] AS last
  FROM orders
  WHERE type <> 'Wholesale'
  #AND days_to_order >= 0
  GROUP BY commission_user_id
), firsts AS (
#-##
  SELECT
    ao.commission_user_id,
    ao.first.days_to_order,
    ao.first.order_id
  FROM alpha_omega ao
  LEFT JOIN orders o
    ON ao.commission_user_id = o.commission_user_id
    AND o.order_id IS NULL
  WHERE o.commission_user_id IS NULL
), summ AS (
  SELECT
    days_to_order,
    COUNT(DISTINCT commission_user_id) AS num
  FROM firsts
  GROUP BY days_to_order
  HAVING COUNT(*) > 0
  ORDER BY days_to_order ASC
)
SELECT
  days_to_order AS days_to_first_sale,
  num,
  num / SUM(num) OVER () AS percent_of_total,
  SUM(num) OVER (ORDER BY days_to_order ASC) AS running_total,
  SUM(num) OVER (ORDER BY days_to_order ASC) / SUM(num) OVER () AS percent_of_running_total,
  SUM(IF(days_to_order > -1, num, NULL)) OVER (ORDER BY days_to_order ASC) AS running_total_from_zero,
  SUM(IF(days_to_order > -1, num, NULL)) OVER (ORDER BY days_to_order ASC) / SUM(IF(days_to_order > -1, num, NULL)) OVER () AS percent_of_running_total_from_zero
FROM summ
