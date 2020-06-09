WITH
##-# users
users AS (
    SELECT
        tree_user_id,
        FORMAT_DATE('%Y', DATE(created)) AS `year`,
        FORMAT_DATE('%m', DATE(created)) AS `month`,
        DATE(created) AS signup
    FROM staging.users
    WHERE created BETWEEN '2018-07-01' AND '2019-06-30'
    AND type = 'Market Partner'
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
    LEFT JOIN staging.orders s
        ON s.commission_user_id = u.tree_user_id
        AND  s.type IN ('Retail', 'Wholesale')
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
  WHERE type = 'Retail'
  AND status <> 'Cancelled'
  AND signup BETWEEN '2018-07-01' AND '2019-06-30'
  AND days_to_order >= 0
  GROUP BY commission_user_id
),
#-##
##-# sales
sales AS (
  SELECT
      year,
      month,
      ao.first.days_to_order_bucket,
      ao.first.bucket_order,
      COUNT(DISTINCT IF(type = 'Retail', o.commission_user_id, NULL)) AS mp_count,
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
  GROUP BY year, month, ao.first.bucket_order, ao.first.days_to_order_bucket
  ORDER BY year, month, ao.first.bucket_order
),
#-##
##-# alpha_omega_agg
alpha_omega_agg AS (
    SELECT
        u.year,
        u.month,
        first.days_to_order_bucket,
        AVG(first.days_to_order) AS avg_days_to_first_order,
        MIN(first.days_to_order) AS min_days_to_first_order,
        MAX(first.days_to_order) AS max_days_to_first_order,
        AVG(last.days_to_order) AS avg_days_to_last_order,
        MIN(last.days_to_order) AS min_days_to_last_order,
        MAX(last.days_to_order) AS max_days_to_last_order
    FROM alpha_omega ao
    INNER JOIN users u ON ao.commission_user_id = u.tree_user_id
    GROUP BY u.year, u.month, first.bucket_order, first.days_to_order_bucket
    ORDER BY u.year, u.month, first.bucket_order
),
#-##
##-# summ
summ AS (
    SELECT
        s.year,
        s.month,
        s.days_to_order_bucket,
        s.mp_count,
        s.mp_count / SUM(s.mp_count) OVER (PARTITION BY s.year, s.month) AS mp_percent_of_total,
        SUM(s.mp_count) OVER (PARTITION BY s.year, s.month ORDER BY s.bucket_order) AS mp_running_total,
        SUM(s.mp_count) OVER (PARTITION BY s.year, s.month ORDER BY s.bucket_order) / SUM(s.mp_count) OVER (PARTITION BY s.year, s.month) AS mp_percent_of_running_total,
        ##-# days to first,last order
        oao.min_days_to_first_order,
        oao.max_days_to_first_order,
        oao.avg_days_to_first_order,
        oao.avg_days_to_last_order,
        oao.min_days_to_last_order,
        oao.max_days_to_last_order,
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
        IF(SUM(total) OVER (PARTITION BY s.year, s.month) > 0, total / SUM(total) OVER (PARTITION BY s.year, s.month), NULL) AS percent_of_month_total,
        #-##
        ##-# < 30
        _30_sales_count,
        IF(sales_count > 0, _30_sales_count / sales_count, NULL) AS _30_sales_count_percent,
        IF(_30_sales_count > 0,  _30_sales_count / _30_sales_user_count, NULL)  AS _30_avg_sales_count_user,
        _30_sales_total,
        IF(sales_total > 0, _30_sales_total / sales_total, NULL) AS _30_sales_total_percent,
        IF(_30_sales_total > 0, _30_sales_total / _30_sales_count, NULL) AS _30_avg_sales_rev_sale,
        IF(_30_sales_total > 0, _30_sales_total / _30_sales_user_count, NULL) AS _30_avg_sales_rev_user,
        _30_wholesale_count,
        IF(wholesale_count > 0, _30_wholesale_count / wholesale_count, NULL) AS _30_wholesale_count_percent,
        IF(_30_wholesale_count > 0, _30_wholesale_count / _30_wholesale_user_count, NULL) AS _30_avg_wholesale_count_user,
        _30_wholesale_total,
        IF(wholesale_total > 0, _30_wholesale_total / wholesale_total, NULL) AS _30_wholesale_total_percent,
        IF(_30_wholesale_total > 0, _30_wholesale_total / _30_wholesale_count, NULL) AS _30_avg_wholesale_rev_sale,
        IF(_30_wholesale_total > 0, _30_wholesale_total / _30_wholesale_user_count, NULL) AS _30_wholesale_rev_user,
        _30_count,
        IF(count > 0, _30_count / count, NULL) AS _30_percent_of_total_count,
        IF(_30_count > 0,  _30_count / _30_user_count, NULL)  AS _30_avg_count_user,
        _30_total,
        IF(total > 0, _30_total / total, NULL) AS _30_percent_of_total_rev,
        IF(_30_total > 0, _30_total / _30_count, NULL) AS _30_avg_rev_sale,
        IF(_30_total > 0, _30_total / _30_user_count, NULL) AS _30_avg_rev_user,
        IF(SUM(_30_total) OVER (PARTITION BY s.year, s.month) > 0, _30_total / SUM(_30_total) OVER (PARTITION BY s.year, s.month), NULL) AS _30_percent_of_month_total,
        #-##
        ##-# 31 - 90
        _31_90_sales_count,
        IF(_31_90_sales_count > 0,  _31_90_sales_count / _31_90_sales_user_count, NULL)  AS _31_90_avg_sales_count_user,
        _31_90_sales_total,
        IF(_31_90_sales_total > 0, _31_90_sales_total / _31_90_sales_count, NULL) AS _31_90_avg_sales_rev_sale,
        IF(_31_90_sales_total > 0, _31_90_sales_total / _31_90_sales_user_count, NULL) AS _31_90_avg_sales_rev_user,
        _31_90_wholesale_count,
        IF(_31_90_wholesale_count > 0, _31_90_wholesale_count / _31_90_wholesale_user_count, NULL) AS _31_90_avg_wholesale_count_user,
        _31_90_wholesale_total,
        IF(_31_90_wholesale_total > 0, _31_90_wholesale_total / _31_90_wholesale_count, NULL) AS _31_90_avg_wholesale_rev_sale,
        IF(_31_90_wholesale_total > 0, _31_90_wholesale_total / _31_90_wholesale_user_count, NULL) AS _31_90_wholesale_rev_user,
        _31_90_count,
        IF(count > 0, _31_90_count / count, NULL) AS _31_90_percent_of_total_count,
        IF(_31_90_count > 0,  _31_90_count / _31_90_user_count, NULL)  AS _31_90_avg_count_user,
        _31_90_total,
        IF(total > 0, _31_90_total / total, NULL) AS _31_90_percent_of_total_rev,
        IF(_31_90_total > 0, _31_90_total / _31_90_count, NULL) AS _31_90_avg_rev_sale,
        IF(_31_90_total > 0, _31_90_total / _31_90_user_count, NULL) AS _31_90_avg_rev_user,
        IF(SUM(_31_90_total) OVER (PARTITION BY s.year, s.month) > 0, _31_90_total / SUM(_31_90_total) OVER (PARTITION BY s.year, s.month), NULL) AS _31_90_percent_of_month_total,
        #-##
        ##-# 91 - 180
        _91_180_sales_count,
        IF(_91_180_sales_count > 0,  _91_180_sales_count / _91_180_sales_user_count, NULL)  AS _91_180_avg_sales_count_user,
        _91_180_sales_total,
        IF(_91_180_sales_total > 0, _91_180_sales_total / _91_180_sales_count, NULL) AS _91_180_avg_sales_rev_sale,
        IF(_91_180_sales_total > 0, _91_180_sales_total / _91_180_sales_user_count, NULL) AS _91_180_avg_sales_rev_user,
        _91_180_wholesale_count,
        IF(_91_180_wholesale_count > 0, _91_180_wholesale_count / _91_180_wholesale_user_count, NULL) AS _91_180_avg_wholesale_count_user,
        _91_180_wholesale_total,
        IF(_91_180_wholesale_total > 0, _91_180_wholesale_total / _91_180_wholesale_count, NULL) AS _91_180_avg_wholesale_rev_sale,
        IF(_91_180_wholesale_total > 0, _91_180_wholesale_total / _91_180_wholesale_user_count, NULL) AS _91_180_wholesale_rev_user,
        _91_180_count,
        IF(count > 0, _91_180_count / count, NULL) AS _91_180_percent_of_total_count,
        IF(_91_180_count > 0,  _91_180_count / _91_180_user_count, NULL)  AS _91_180_avg_count_user,
        _91_180_total,
        IF(total > 0, _91_180_total / total, NULL) AS _91_180_percent_of_total_rev,
        IF(_91_180_total > 0, _91_180_total / _91_180_count, NULL) AS _91_180_avg_rev_sale,
        IF(_91_180_total > 0, _91_180_total / _91_180_user_count, NULL) AS _91_180_avg_rev_user,
        IF(SUM(_91_180_total) OVER (PARTITION BY s.year, s.month) > 0, _91_180_total / SUM(_91_180_total) OVER (PARTITION BY s.year, s.month), NULL) AS _91_180_percent_of_month_total,
        #-##
        ##-# 181 - 365
        _181_365_sales_count,
        IF(_181_365_sales_count > 0,  _181_365_sales_count / _181_365_sales_user_count, NULL)  AS _181_365_avg_sales_count_user,
        _181_365_sales_total,
        IF(_181_365_sales_total > 0, _181_365_sales_total / _181_365_sales_count, NULL) AS _181_365_avg_sales_rev_sale,
        IF(_181_365_sales_total > 0, _181_365_sales_total / _181_365_sales_user_count, NULL) AS _181_365_avg_sales_rev_user,
        _181_365_wholesale_count,
        IF(_181_365_wholesale_count > 0, _181_365_wholesale_count / _181_365_wholesale_user_count, NULL) AS _181_365_avg_wholesale_count_user,
        _181_365_wholesale_total,
        IF(_181_365_wholesale_total > 0, _181_365_wholesale_total / _181_365_wholesale_count, NULL) AS _181_365_avg_wholesale_rev_sale,
        IF(_181_365_wholesale_total > 0, _181_365_wholesale_total / _181_365_wholesale_user_count, NULL) AS _181_365_wholesale_rev_user,
        _181_365_count,
        IF(count > 0, _181_365_count / count, NULL) AS _181_365_percent_of_total_count,
        IF(_181_365_count > 0,  _181_365_count / _181_365_user_count, NULL)  AS _181_365_avg_count_user,
        _181_365_total,
        IF(total > 0, _181_365_total / total, NULL) AS _181_365_percent_of_total_rev,
        IF(_181_365_total > 0, _181_365_total / _181_365_count, NULL) AS _181_365_avg_rev_sale,
        IF(_181_365_total > 0, _181_365_total / _181_365_user_count, NULL) AS _181_365_avg_rev_user,
        IF(SUM(_181_365_total) OVER (PARTITION BY s.year, s.month) > 0, _181_365_total / SUM(_181_365_total) OVER (PARTITION BY s.year, s.month), NULL) AS _181_365_percent_of_month_total,
        #-##
        ##-# 366 - 730
        _366_730_sales_count,
        IF(_366_730_sales_count > 0,  _366_730_sales_count / _366_730_sales_user_count, NULL)  AS _366_730_avg_sales_count_user,
        _366_730_sales_total,
        IF(_366_730_sales_total > 0, _366_730_sales_total / _366_730_sales_count, NULL) AS _366_730_avg_sales_rev_sale,
        IF(_366_730_sales_total > 0, _366_730_sales_total / _366_730_sales_user_count, NULL) AS _366_730_avg_sales_rev_user,
        _366_730_wholesale_count,
        IF(_366_730_wholesale_count > 0, _366_730_wholesale_count / _366_730_wholesale_user_count, NULL) AS _366_730_avg_wholesale_count_user,
        _366_730_wholesale_total,
        IF(_366_730_wholesale_total > 0, _366_730_wholesale_total / _366_730_wholesale_count, NULL) AS _366_730_avg_wholesale_rev_sale,
        IF(_366_730_wholesale_total > 0, _366_730_wholesale_total / _366_730_wholesale_user_count, NULL) AS _366_730_wholesale_rev_user,
        _366_730_count,
        IF(count > 0, _366_730_count / count, NULL) AS _366_730_percent_of_total_count,
        IF(_366_730_count > 0,  _366_730_count / _366_730_user_count, NULL)  AS _366_730_avg_count_user,
        _366_730_total,
        IF(total > 0, _366_730_total / total, NULL) AS _366_730_percent_of_total_rev,
        IF(_366_730_total > 0, _366_730_total / _366_730_count, NULL) AS _366_730_avg_rev_sale,
        IF(_366_730_total > 0, _366_730_total / _366_730_user_count, NULL) AS _366_730_avg_rev_user,
        IF(SUM(_366_730_total) OVER (PARTITION BY s.year, s.month) > 0, _366_730_total / SUM(_366_730_total) OVER (PARTITION BY s.year, s.month), NULL) AS _366_730_percent_of_month_total,
        #-##
        ##-# > 730
        _730_sales_count,
        IF(_730_sales_count > 0,  _730_sales_count / _730_sales_user_count, NULL)  AS _730_avg_sales_count_user,
        _730_sales_total,
        IF(_730_sales_total > 0, _730_sales_total / _730_sales_count, NULL) AS _730_avg_sales_rev_sale,
        IF(_730_sales_total > 0, _730_sales_total / _730_sales_user_count, NULL) AS _730_avg_sales_rev_user,
        _730_wholesale_count,
        IF(_730_wholesale_count > 0, _730_wholesale_count / _730_wholesale_user_count, NULL) AS _730_avg_wholesale_count_user,
        _730_wholesale_total,
        IF(_730_wholesale_total > 0, _730_wholesale_total / _730_wholesale_count, NULL) AS _730_avg_wholesale_rev_sale,
        IF(_730_wholesale_total > 0, _730_wholesale_total / _730_wholesale_user_count, NULL) AS _730_wholesale_rev_user,
        _730_count,
        IF(count > 0, _730_count / count, NULL) AS _730_percent_of_total_count,
        IF(_730_count > 0,  _730_count / _730_user_count, NULL)  AS _730_avg_count_user,
        _730_total,
        IF(total > 0, _730_total / total, NULL) AS _730_percent_of_total_rev,
        IF(_730_total > 0, _730_total / _730_count, NULL) AS _730_avg_rev_sale,
        IF(_730_total > 0, _730_total / _730_user_count, NULL) AS _730_avg_rev_user,
        IF(SUM(_730_total) OVER (PARTITION BY s.year, s.month) > 0, _730_total / SUM(_730_total) OVER (PARTITION BY s.year, s.month), NULL) AS _730_percent_of_month_total
        #-##
    FROM sales s
    LEFT JOIN alpha_omega_agg oao
        ON s.month = oao.month
        AND s.year = oao.year
        AND s.days_to_order_bucket = oao.days_to_order_bucket
)
#-##
SELECT CONCAT(month, '/', year), * FROM summ
##-# SELECT
/*SELECT
    SUM(total) OVER () AS agg_rev_total,
    SUM((
        IFNULL((_30_avg_sales_count_user*.02)*_30_avg_sales_rev_sale*mp_count, 0)+IFNULL((_30_avg_wholesale_count_user*.02)*_30_avg_wholesale_rev_sale*mp_count, 0)
        + (IFNULL((_31_90_avg_sales_count_user*.02)*_31_90_avg_sales_rev_sale*mp_count, 0)+IFNULL((_31_90_avg_wholesale_count_user*.02)*_31_90_avg_wholesale_rev_sale*mp_count, 0))
        + (IFNULL((_91_180_avg_sales_count_user*.02)*_91_180_avg_sales_rev_sale*mp_count, 0)+IFNULL((_91_180_avg_wholesale_count_user*.02)*_91_180_avg_wholesale_rev_sale*mp_count, 0))
        + (IFNULL((_181_365_avg_sales_count_user*.02)*_181_365_avg_sales_rev_sale*mp_count, 0)+IFNULL((_181_365_avg_wholesale_count_user*.02)*_181_365_avg_wholesale_rev_sale*mp_count, 0))
        + (IFNULL((_366_730_avg_sales_count_user*.02)*_366_730_avg_sales_rev_sale*mp_count, 0)+IFNULL((_366_730_avg_wholesale_count_user*.02)*_366_730_avg_wholesale_rev_sale*mp_count, 0))
    )) OVER () AS agg_rev_increase,
    mp_count,
    _30_avg_sales_count_user,
    _30_avg_sales_rev_sale,
    _30_avg_wholesale_count_user,
    _30_avg_wholesale_rev_sale,
    _30_total,
    (_30_avg_sales_count_user*.02)*_30_avg_sales_rev_sale*mp_count AS _30_sales_rev_increase,
    (_30_avg_wholesale_count_user*.02)*_30_avg_wholesale_rev_sale*mp_count AS _30_wholesale_rev_increase,
    (IFNULL((_30_avg_sales_count_user*.02)*_30_avg_sales_rev_sale*mp_count, 0)+IFNULL((_30_avg_wholesale_count_user*.02)*_30_avg_wholesale_rev_sale*mp_count, 0)) AS _30_total_rev_increase,
    SUM((IFNULL((_30_avg_sales_count_user*.02)*_30_avg_sales_rev_sale*mp_count, 0)+IFNULL((_30_avg_wholesale_count_user*.02)*_30_avg_wholesale_rev_sale*mp_count, 0))) OVER () AS _30_agg_rev_increase,
    _31_90_avg_sales_count_user,
    _31_90_avg_sales_rev_sale,
    _31_90_avg_wholesale_count_user,
    _31_90_avg_wholesale_rev_sale,
    _31_90_total,
    (_31_90_avg_sales_count_user*.02)*_31_90_avg_sales_rev_sale*mp_count AS _31_90_sales_rev_increase,
    (_31_90_avg_wholesale_count_user*.02)*_31_90_avg_wholesale_rev_sale*mp_count AS _31_90_wholesale_rev_increase,
    (IFNULL((_31_90_avg_sales_count_user*.02)*_31_90_avg_sales_rev_sale*mp_count, 0)+IFNULL((_31_90_avg_wholesale_count_user*.02)*_31_90_avg_wholesale_rev_sale*mp_count, 0)) AS _31_90_total_rev_increase,
    SUM((IFNULL((_31_90_avg_sales_count_user*.02)*_31_90_avg_sales_rev_sale*mp_count, 0)+IFNULL((_31_90_avg_wholesale_count_user*.02)*_31_90_avg_wholesale_rev_sale*mp_count, 0))) OVER () AS _31_90_agg_rev_increase,
    _91_180_avg_sales_count_user,
    _91_180_avg_sales_rev_sale,
    _91_180_avg_wholesale_count_user,
    _91_180_avg_wholesale_rev_sale,
    _91_180_total,
    (_91_180_avg_sales_count_user*.02)*_91_180_avg_sales_rev_sale*mp_count AS _91_180_sales_rev_increase,
    (_91_180_avg_wholesale_count_user*.02)*_91_180_avg_wholesale_rev_sale*mp_count AS _91_180_wholesale_rev_increase,
    (IFNULL((_91_180_avg_sales_count_user*.02)*_91_180_avg_sales_rev_sale*mp_count, 0)+IFNULL((_91_180_avg_wholesale_count_user*.02)*_91_180_avg_wholesale_rev_sale*mp_count, 0)) AS _91_180_total_rev_increase,
    SUM((IFNULL((_91_180_avg_sales_count_user*.02)*_91_180_avg_sales_rev_sale*mp_count, 0)+IFNULL((_91_180_avg_wholesale_count_user*.02)*_91_180_avg_wholesale_rev_sale*mp_count, 0))) OVER () AS _91_180_agg_rev_increase,
    _181_365_avg_sales_count_user,
    _181_365_avg_sales_rev_sale,
    _181_365_avg_wholesale_count_user,
    _181_365_avg_wholesale_rev_sale,
    _181_365_total,
    (_181_365_avg_sales_count_user*.02)*_181_365_avg_sales_rev_sale*mp_count AS _181_365_sales_rev_increase,
    (_181_365_avg_wholesale_count_user*.02)*_181_365_avg_wholesale_rev_sale*mp_count AS _181_365_wholesale_rev_increase,
    (IFNULL((_181_365_avg_sales_count_user*.02)*_181_365_avg_sales_rev_sale*mp_count, 0)+IFNULL((_181_365_avg_wholesale_count_user*.02)*_181_365_avg_wholesale_rev_sale*mp_count, 0)) AS _181_365_total_rev_increase,
    SUM((IFNULL((_181_365_avg_sales_count_user*.02)*_181_365_avg_sales_rev_sale*mp_count, 0)+IFNULL((_181_365_avg_wholesale_count_user*.02)*_181_365_avg_wholesale_rev_sale*mp_count, 0))) OVER () AS _181_365_agg_rev_increase,
    _366_730_avg_sales_count_user,
    _366_730_avg_sales_rev_sale,
    _366_730_avg_wholesale_count_user,
    _366_730_avg_wholesale_rev_sale,
    _366_730_total,
    (_366_730_avg_sales_count_user*.02)*_366_730_avg_sales_rev_sale*mp_count AS _366_730_sales_rev_increase,
    (_366_730_avg_wholesale_count_user*.02)*_366_730_avg_wholesale_rev_sale*mp_count AS _366_730_wholesale_rev_increase,
    (IFNULL((_366_730_avg_sales_count_user*.02)*_366_730_avg_sales_rev_sale*mp_count, 0)+IFNULL((_366_730_avg_wholesale_count_user*.02)*_366_730_avg_wholesale_rev_sale*mp_count, 0)) AS _366_730_total_rev_increase,
    SUM((IFNULL((_366_730_avg_sales_count_user*.02)*_366_730_avg_sales_rev_sale*mp_count, 0)+IFNULL((_366_730_avg_wholesale_count_user*.02)*_366_730_avg_wholesale_rev_sale*mp_count, 0))) OVER () AS _366_730_agg_rev_increase
FROM summ
*/
#-##
