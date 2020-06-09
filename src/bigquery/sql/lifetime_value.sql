WITH
##-# users
users AS (
    SELECT
        tree_user_id,
        FORMAT_DATE('%Y', DATE(created)) AS `year`,
        FORMAT_DATE('%m', DATE(created)) AS `month`,
        DATE(created) AS signup
    FROM vibe_staging.users
    WHERE created BETWEEN '2018-07-01' AND '2019-06-30'
    AND type = 'Distributor'
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
  AND days_to_order >= 0
  GROUP BY commission_user_id
),
#-##
##-# sales
sales AS (
  SELECT
      ao.first.days_to_order_bucket,
      ao.first.bucket_order,
      COUNT(DISTINCT o.commission_user_id) AS mp_count,
      COUNT(DISTINCT IF(type = 'Retail', o.commission_user_id, NULL)) AS retail_mp_count,
      COUNT(DISTINCT IF(type = 'Wholesale', o.commission_user_id, NULL)) AS wholesale_mp_count,
      ##-# Total
      COUNT(IF(type = 'Retail', order_id, NULL)) AS sales_count,
      COUNT(IF(type = 'Wholesale', order_id, NULL)) AS wholesale_count,
      COUNT(IF(type IN ('Retail', 'Wholesale'), order_id, NULL)) AS count,
      SUM(IF(type = 'Retail', total, NULL)) AS sales_total,
      SUM(IF(type = 'Wholesale', total, NULL)) AS wholesale_total,
      SUM(IF(type IN ('Retail', 'Wholesale'), total, NULL)) AS total,
      COUNT(DISTINCT IF(type = 'Retail', o.commission_user_id, NULL)) AS sales_user_count,
      COUNT(DISTINCT IF(type = 'Wholesale', o.commission_user_id, NULL)) AS wholesale_user_count,
      COUNT(DISTINCT IF(type IN ('Retail', 'Wholesale'), o.commission_user_id, NULL)) AS user_count,
      #-##
      ##-# < 30
      COUNT(IF(type = 'Retail' AND days_to_order BETWEEN 0 AND 30, order_id, NULL)) AS _30_sales_count,
      COUNT(IF(type = 'Wholesale' AND days_to_order BETWEEN 0 AND 30, order_id, NULL)) AS _30_wholesale_count,
      COUNT(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 0 AND 30, order_id, NULL)) AS _30_count,
      SUM(IF(type = 'Retail' AND days_to_order BETWEEN 0 AND 30, total, NULL)) AS _30_sales_total,
      SUM(IF(type = 'Wholesale' AND days_to_order BETWEEN 0 AND 30, total, NULL)) AS _30_wholesale_total,
      SUM(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 0 AND 30, total, NULL)) AS _30_total,
      COUNT(DISTINCT IF(type = 'Retail' AND days_to_order BETWEEN 0 AND 30, o.commission_user_id, NULL)) AS _30_sales_user_count,
      COUNT(DISTINCT IF(type = 'Wholesale' AND days_to_order BETWEEN 0 AND 30, o.commission_user_id, NULL)) AS _30_wholesale_user_count,
      COUNT(DISTINCT IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 0 AND 30, o.commission_user_id, NULL)) AS _30_user_count,
      #-##
      ##-# 31 -90
      COUNT(IF(type = 'Retail' AND days_to_order BETWEEN 31 AND 90, order_id, NULL)) AS _31_90_sales_count,
      COUNT(IF(type = 'Wholesale' AND days_to_order BETWEEN 31 AND 90, order_id, NULL)) AS _31_90_wholesale_count,
      COUNT(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 31 AND 90, order_id, NULL)) AS _31_90_count,
      SUM(IF(type = 'Retail' AND days_to_order BETWEEN 31 AND 90, total, NULL)) AS _31_90_sales_total,
      SUM(IF(type = 'Wholesale' AND days_to_order BETWEEN 31 AND 90, total, NULL)) AS _31_90_wholesale_total,
      SUM(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 31 AND 90, total, NULL)) AS _31_90_total,
      COUNT(DISTINCT IF(type = 'Retail' AND days_to_order BETWEEN 31 AND 90, o.commission_user_id, NULL)) AS _31_90_sales_user_count,
      COUNT(DISTINCT IF(type = 'Wholesale' AND days_to_order BETWEEN 31 AND 90, o.commission_user_id, NULL)) AS _31_90_wholesale_user_count,
      COUNT(DISTINCT IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 31 AND 90, o.commission_user_id, NULL)) AS _31_90_user_count,
      #-##
      ##-# 91-180
      COUNT(IF(type = 'Retail' AND days_to_order BETWEEN 91 AND 180, order_id, NULL)) AS _91_180_sales_count,
      COUNT(IF(type = 'Wholesale' AND days_to_order BETWEEN 91 AND 180, order_id, NULL)) AS _91_180_wholesale_count,
      COUNT(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 91 AND 180, order_id, NULL)) AS _91_180_count,
      SUM(IF(type = 'Retail' AND days_to_order BETWEEN 91 AND 180, total, NULL)) AS _91_180_sales_total,
      SUM(IF(type = 'Wholesale' AND days_to_order BETWEEN 91 AND 180, total, NULL)) AS _91_180_wholesale_total,
      SUM(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 91 AND 180, total, NULL)) AS _91_180_total,
      COUNT(DISTINCT IF(type = 'Retail' AND days_to_order BETWEEN 91 AND 180, o.commission_user_id, NULL)) AS _91_180_sales_user_count,
      COUNT(DISTINCT IF(type = 'Wholesale' AND days_to_order BETWEEN 91 AND 180, o.commission_user_id, NULL)) AS _91_180_wholesale_user_count,
      COUNT(DISTINCT IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 91 AND 180, o.commission_user_id, NULL)) AS _91_180_user_count,
      #-##
      ##-# 181-365
      COUNT(IF(type = 'Retail' AND days_to_order BETWEEN 181 AND 365, order_id, NULL)) AS _181_365_sales_count,
      COUNT(IF(type = 'Wholesale' AND days_to_order BETWEEN 181 AND 365, order_id, NULL)) AS _181_365_wholesale_count,
      COUNT(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 181 AND 365, order_id, NULL)) AS _181_365_count,
      SUM(IF(type = 'Retail' AND days_to_order BETWEEN 181 AND 365, total, NULL)) AS _181_365_sales_total,
      SUM(IF(type = 'Wholesale' AND days_to_order BETWEEN 181 AND 365, total, NULL)) AS _181_365_wholesale_total,
      SUM(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 181 AND 365, total, NULL)) AS _181_365_total,
      COUNT(DISTINCT IF(type = 'Retail' AND days_to_order BETWEEN 181 AND 365, o.commission_user_id, NULL)) AS _181_365_sales_user_count,
      COUNT(DISTINCT IF(type = 'Wholesale' AND days_to_order BETWEEN 181 AND 365, o.commission_user_id, NULL)) AS _181_365_wholesale_user_count,
      COUNT(DISTINCT IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 181 AND 365, o.commission_user_id, NULL)) AS _181_365_user_count,
      #-##
      ##-# 366-730
      COUNT(IF(type = 'Retail' AND days_to_order BETWEEN 366 AND 730, order_id, NULL)) AS _366_730_sales_count,
      COUNT(IF(type = 'Wholesale' AND days_to_order BETWEEN 366 AND 730, order_id, NULL)) AS _366_730_wholesale_count,
      COUNT(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 366 AND 730, order_id, NULL)) AS _366_730_count,
      SUM(IF(type = 'Retail' AND days_to_order BETWEEN 366 AND 730, total, NULL)) AS _366_730_sales_total,
      SUM(IF(type = 'Wholesale' AND days_to_order BETWEEN 366 AND 730, total, NULL)) AS _366_730_wholesale_total,
      SUM(IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 366 AND 730, total, NULL)) AS _366_730_total,
      COUNT(DISTINCT IF(type = 'Retail' AND days_to_order BETWEEN 366 AND 730, o.commission_user_id, NULL)) AS _366_730_sales_user_count,
      COUNT(DISTINCT IF(type = 'Wholesale' AND days_to_order BETWEEN 366 AND 730, o.commission_user_id, NULL)) AS _366_730_wholesale_user_count,
      COUNT(DISTINCT IF(type IN ('Retail', 'Wholesale') AND days_to_order BETWEEN 366 AND 730, o.commission_user_id, NULL)) AS _366_730_user_count,
      #-##
      ##-# > 730
      COUNT(IF(type = 'Retail' AND days_to_order > 730, order_id, NULL)) AS _730_sales_count,
      COUNT(IF(type = 'Wholesale' AND days_to_order > 730, order_id, NULL)) AS _730_wholesale_count,
      COUNT(IF(type IN ('Retail', 'Wholesale') AND days_to_order > 730, order_id, NULL)) AS _730_count,
      SUM(IF(type = 'Retail' AND days_to_order > 730, total, NULL)) AS _730_sales_total,
      SUM(IF(type = 'Wholesale' AND days_to_order > 730, total, NULL)) AS _730_wholesale_total,
      SUM(IF(type IN ('Retail', 'Wholesale') AND days_to_order > 730, total, NULL)) AS _730_total,
      COUNT(DISTINCT IF(type = 'Retail' AND days_to_order > 730, o.commission_user_id, NULL)) AS _730_sales_user_count,
      COUNT(DISTINCT IF(type = 'Wholesale' AND days_to_order > 730, o.commission_user_id, NULL)) AS _730_wholesale_user_count,
      COUNT(DISTINCT IF(type IN ('Retail', 'Wholesale') AND days_to_order > 730, o.commission_user_id, NULL)) AS _730_user_count
      #-##
  FROM orders o
  LEFT JOIN alpha_omega ao ON o.commission_user_id = ao.commission_user_id
  WHERE (o.days_to_order >= 0 OR o.days_to_order IS NULL)
  GROUP BY ao.first.bucket_order, ao.first.days_to_order_bucket
  ORDER BY ao.first.bucket_order
),
#-##
##-# alpha_omega_agg
alpha_omega_agg AS (
    SELECT
        first.bucket_order,
        first.days_to_order_bucket,
        AVG(first.days_to_order) AS avg_days_to_first_order,
        MIN(first.days_to_order) AS min_days_to_first_order,
        MAX(first.days_to_order) AS max_days_to_first_order,
        AVG(last.days_to_order) AS avg_days_to_last_order,
        MIN(last.days_to_order) AS min_days_to_last_order,
        MAX(last.days_to_order) AS max_days_to_last_order
    FROM alpha_omega ao
    INNER JOIN users u ON ao.commission_user_id = u.tree_user_id
    GROUP BY first.bucket_order, first.days_to_order_bucket
    ORDER BY first.bucket_order
)
#-##
SELECT
    s.days_to_order_bucket,
    s.mp_count,
    s.mp_count / SUM(s.mp_count) OVER () AS mp_percent_of_total,
    SUM(s.mp_count) OVER (ORDER BY s.bucket_order) AS mp_running_total,
    SUM(s.mp_count) OVER (ORDER BY s.bucket_order) / SUM(s.mp_count) OVER () AS mp_percent_of_running_total,
    ##-# days to first,last order
    oao.avg_days_to_first_order,
    oao.avg_days_to_last_order,
    #-##
    ##-# total
    sales_count,
    IF(sales_count > 0, sales_count / sales_user_count, NULL) AS avg_sales_count_user,
    sales_total,
    IF(sales_total > 0, sales_total / sales_count, NULL) AS avg_sales_rev_sale,
    IF(sales_total > 0, sales_total / sales_user_count, NULL) AS avg_sales_rev_user,
    wholesale_count,
    IF(wholesale_count > 0, wholesale_count / wholesale_user_count, NULL) AS avg_wholesale_count_user,
    wholesale_total,
    IF(wholesale_total > 0, wholesale_total / wholesale_count, NULL) AS avg_wholesale_rev_sale,
    IF(wholesale_total > 0, wholesale_total / wholesale_user_count, NULL) AS avg_wholesale_rev_user,
    count,
    IF(count > 0, count / user_count, NULL) AS avg_count_user,
    total,
    IF(total > 0, total / count, NULL) AS avg_rev_sale,
    IF(total > 0, total / user_count, NULL) AS avg_rev_user,
    IF(SUM(total) OVER () > 0, total / SUM(total) OVER (), NULL) AS percent_of_total
    #-##
FROM sales s
LEFT JOIN alpha_omega_agg oao
    ON s.days_to_order_bucket = oao.days_to_order_bucket
#-##
