WITH
##-# users
users AS (
    SELECT
        tree_user_id,
        user_id,
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
    ao.*,
    DATE_DIFF(CURRENT_DATE, ao.signup, DAY) AS days_since_signup,
    ao.first_retail.days_to_order AS days_to_first_retail,
    ao.last_retail.days_to_order AS days_to_last_retail,
    DATE_DIFF(DATE(ao.last_retail.order_date), DATE(ao.first_retail.order_date), DAY) AS days_active_retail,
    ao.first_autoship.days_to_order AS days_to_first_autoship,
    ao.last_autoship.days_to_order AS days_to_last_autoship,
    DATE_DIFF(DATE(ao.last_autoship.order_date), DATE(ao.first_autoship.order_date), DAY) AS days_active_autoship,
    ao.first_wholesale.days_to_order AS days_to_first_wholesale,
    ao.last_wholesale.days_to_order AS days_to_last_wholesale,
    DATE_DIFF(DATE(ao.last_wholesale.order_date), DATE(ao.first_wholesale.order_date), DAY) AS days_active_wholesale
  FROM (
    SELECT
      commission_user_id,
      signup,
      ARRAY_AGG(IF(type = 'Retail', STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) IGNORE NULLS ORDER BY order_date ASC lIMIT 1)[OFFSET (0)] AS first_retail,
      ARRAY_AGG(IF(type = 'Retail', STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) ORDER BY order_date DESC lIMIT 1)[OFFSET (0)] AS last_retail,
      ARRAY_AGG(IF(type = 'Wholesale' AND o.commission_user_id = o.tree_user_id, STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) IGNORE NULLS ORDER BY order_date ASC lIMIT 1)[OFFSET (0)] AS first_wholesale,
      ARRAY_AGG(IF(type = 'Wholesale' AND o.commission_user_id = o.tree_user_id, STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) ORDER BY order_date DESC lIMIT 1)[OFFSET (0)] AS last_wholesale,
      ARRAY_AGG(IF(type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id, STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) IGNORE NULLS ORDER BY order_date ASC lIMIT 1)[OFFSET (0)] AS first_autoship,
      ARRAY_AGG(IF(type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id, STRUCT(order_id, order_date, total, days_to_order, days_to_order_bucket, bucket_order), NULL) ORDER BY order_date DESC lIMIT 1)[OFFSET (0)] AS last_autoship
    FROM orders o
    WHERE o.days_to_order >= 0
    GROUP BY o.commission_user_id, o.signup
  ) ao
),
#-##
##-# order_metrics
order_metrics AS (
  SELECT
    o.commission_user_id,
    COUNT(IF(o.type = 'Retail' AND DATE_DIFF(DATE(o.order_date), o.signup, DAY) BETWEEN 0 AND 30, o.order_id, NULL)) AS count_first30_retail,
    SUM(IF(o.type = 'Retail' AND DATE_DIFF(DATE(o.order_date), o.signup, DAY) BETWEEN 0 AND 30, o.total, 0)) AS total_first30_retail,
    COUNT(IF(o.type = 'Wholesale' AND o.commission_user_id = o.tree_user_id AND DATE_DIFF(DATE(o.order_date), o.signup, DAY) BETWEEN 0 AND 30, o.order_id, NULL)) AS count_first30_wholesale,
    SUM(IF(o.type = 'Wholesale' AND o.commission_user_id = o.tree_user_id AND DATE_DIFF(DATE(o.order_date), o.signup, DAY) BETWEEN 0 AND 30, o.total, 0)) AS total_first30_wholesale,
    COUNT(IF(o.type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id AND DATE_DIFF(DATE(o.order_date), o.signup, DAY) BETWEEN 0 AND 30, o.order_id, NULL)) AS count_first30_autoship,
    SUM(IF(o.type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id AND DATE_DIFF(DATE(o.order_date), o.signup, DAY) BETWEEN 0 AND 30, o.total, 0)) AS total_first30_autoship,
    COUNT(IF(o.type = 'Retail' AND DATE_DIFF(CURRENT_DATE, DATE(o.order_date), MONTH) BETWEEN 0 AND 12, o.order_id, NULL)) AS count_last12_retail,
    SUM(IF(o.type = 'Retail' AND DATE_DIFF(CURRENT_DATE, DATE(o.order_date), MONTH) BETWEEN 0 AND 12, o.total, 0)) AS total_last12_retail,
    COUNT(IF(o.type = 'Wholesale' AND o.commission_user_id = o.tree_user_id AND DATE_DIFF(CURRENT_DATE, DATE(o.order_date), MONTH) BETWEEN 0 AND 12, o.order_id, NULL)) AS count_last12_wholesale,
    SUM(IF(o.type = 'Wholesale' AND o.commission_user_id = o.tree_user_id AND DATE_DIFF(CURRENT_DATE, DATE(o.order_date), MONTH) BETWEEN 0 AND 12, o.total, 0)) AS total_last12_wholesale,
    COUNT(IF(o.type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id AND DATE_DIFF(CURRENT_DATE, DATE(o.order_date), MONTH) BETWEEN 0 AND 12, o.order_id, NULL)) AS count_last12_autoship,
    SUM(IF(o.type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id AND DATE_DIFF(CURRENT_DATE, DATE(o.order_date), MONTH) BETWEEN 0 AND 12, o.total, 0)) AS total_last12_autoship,
COUNT(IF(o.type = 'Retail', o.order_id, NULL)) AS count_retail,
    SUM(IF(o.type = 'Retail', o.total, 0)) AS total_retail,
    COUNT(IF(o.type = 'Wholesale' AND o.commission_user_id = o.tree_user_id, o.order_id, NULL)) AS count_wholesale,
    SUM(IF(o.type = 'Wholesale' AND o.commission_user_id = o.tree_user_id, o.total, 0)) AS total_wholesale,
    COUNT(IF(o.type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id, o.order_id, NULL)) AS count_autoship,
    SUM(IF(o.type = 'Wholesale' AND o.commission_user_id <> o.tree_user_id, o.total, 0)) AS total_autoship
  FROM orders o
  LEFT OUTER JOIN alpha_omega ao ON o.commission_user_id = ao.commission_user_id
  WHERE o.days_to_order >= 0
  GROUP BY o.commission_user_id

),
sent_emails AS (
  SELECT
    sender_tree_user_id,
    message_id,
    delivery_status,
    sent_date AS sent,
    message_created_at AS created,
    system_generated,
    COUNT(DISTINCT recipient_tree_user_id) AS recipients,
    MIN(recipients.seen) AS min_seen,
    COUNT(DISTINCT IF(recipients.seen IS NOT NULL, recipient_tree_user_id, NULL)) AS seen_cnt,
    MIN(recipients.read) AS min_read,
    COUNT(DISTINCT IF(recipients.read IS NOT NULL, recipient_tree_user_id, NULL)) AS read_cnt,
    COUNT(DISTINCT IF(recipients.undeliverable IS NOT NULL, recipient_tree_user_id, NULL)) AS undeliverable_cnt
  FROM vibe_staging.emails e
  CROSS JOIN UNNEST(e.recipients) AS recipients
  WHERE icentris_client = 'monat'
  GROUP BY sender_tree_user_id, message_id, delivery_status, sent_date, message_created_at, system_generated
),
#-##
##-# sent_emails_alpha
sent_emails_alpha AS (
  SELECT
    e.sender_tree_user_id,
    DATE_DIFF(MIN(DATE(e.created)), u.signup, DAY) AS days_to_first_created,
    DATE_DIFF(MIN(DATE(e.sent)), u.signup, DAY) AS days_to_first_sent,
    DATE_DIFF(MIN(DATE(e.min_seen)), u.signup, DAY) AS days_to_first_seen,
    DATE_DIFF(MIN(DATE(e.min_read)), u.signup, DAY) AS days_to_first_read,
    IFNULL(COUNT(IF(DATE_DIFF(DATE(e.created), u.signup, DAY) BETWEEN 0 AND 30, e.message_id, NULL)),0) AS cnt_created_first30,
    IFNULL(COUNT(IF(DATE(e.created) >= u.signup AND e.created <= ao.first_retail.order_date, e.message_id, NULL)),0) AS cnt_created_before_first_retail,
    IFNULL(COUNT(IF(DATE(e.created) >= u.signup AND e.created <= ao.first_wholesale.order_date, e.message_id, NULL)),0) AS cnt_created_before_first_wholesale,
    IFNULL(COUNT(IF(DATE(e.created) >= u.signup AND e.created <= ao.first_autoship.order_date, e.message_id, NULL)),0) AS cnt_created_before_first_autoship,
    IFNULL(COUNT(IF(DATE_DIFF(DATE(e.created), u.signup, DAY) BETWEEN 0 AND 30, e.message_id, NULL)),0) AS cnt_sent_first30,
    IFNULL(COUNT(IF(DATE(e.sent) >= u.signup AND e.sent <= ao.first_retail.order_date, e.message_id, NULL)),0) AS cnt_sent_before_first_retail,
    IFNULL(COUNT(IF(DATE(e.sent) >= u.signup AND e.sent <= ao.first_wholesale.order_date, e.message_id, NULL)),0) AS cnt_sent_before_first_wholesale,
    IFNULL(COUNT(IF(DATE(e.sent) >= u.signup AND e.sent <= ao.first_autoship.order_date, e.message_id, NULL)),0) AS cnt_sent_before_first_autoship,
    SUM(IF(DATE_DIFF(DATE(e.created), u.signup, DAY) BETWEEN 0 AND 30, e.seen_cnt, 0)) AS cnt_seen_first30,
    SUM(IF(DATE(e.min_seen) >= u.signup AND e.min_seen <= ao.first_retail.order_date, e.seen_cnt, 0)) AS cnt_seen_before_first_retail,
    SUM(IF(DATE(e.min_seen) >= u.signup AND e.min_seen <= ao.first_wholesale.order_date, e.seen_cnt, 0)) AS cnt_seen_before_first_wholesale,
    SUM(IF(DATE(e.min_seen) >= u.signup AND e.min_seen <= ao.first_autoship.order_date, e.seen_cnt, 0)) AS cnt_seen_before_first_autoship,
    SUM(IF(DATE_DIFF(DATE(e.created), u.signup, DAY) BETWEEN 0 AND 30, e.read_cnt, 0)) AS cnt_read_first30,
    SUM(IF(DATE(e.min_read) >= u.signup AND e.min_read <= ao.first_retail.order_date, e.read_cnt, 0)) AS cnt_read_before_first_retail,
    SUM(IF(DATE(e.min_read) >= u.signup AND e.min_read <= ao.first_wholesale.order_date, e.read_cnt, 0)) AS cnt_readbefore_first_wholesale,
    SUM(IF(DATE(e.min_read) >= u.signup AND e.min_read <= ao.first_autoship.order_date, e.read_cnt, 0)) AS cnt_read_before_first_autoship,
    SUM(IF(DATE_DIFF(DATE(e.created), u.signup, DAY) BETWEEN 0 AND 30, e.undeliverable_cnt, 0)) AS cnt_undeliverable_first30,
    SUM(IF(DATE(e.sent) >= u.signup AND e.sent <= ao.first_retail.order_date, e.undeliverable_cnt, 0)) AS cnt_undeliverable_before_first_retail,
    SUM(IF(DATE(e.sent) >= u.signup AND e.sent <= ao.first_wholesale.order_date, e.undeliverable_cnt, 0)) AS cnt_undeliverable_before_first_wholesale,
    SUM(IF(DATE(e.sent) >= u.signup AND e.sent <= ao.first_autoship.order_date, e.undeliverable_cnt, 0)) AS cnt_undeliverable_before_first_autoship
  FROM users u
  JOIN sent_emails e
    ON u.tree_user_id = e.sender_tree_user_id
  LEFT JOIN alpha_omega ao ON e.sender_tree_user_id = ao.commission_user_id
  WHERE DATE(e.created) >= u.signup
  GROUP BY e.sender_tree_user_id, u.signup
),
#-##
##-# logins
logins AS (
  SELECT
    u.tree_user_id,
    COUNT(IF(lh.platform IS NULL AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_unknown,
    COUNT(IF(lh.platform LIKE 'Android%' AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_android,
    COUNT(IF(lh.platform LIKE 'iPhone%' AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_iphone,
    COUNT(IF(lh.platform LIKE 'iPad%' AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_ipad,
    COUNT(IF(lh.platform = 'Windows' AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_windows,
    COUNT(IF((lh.platform = 'Macintosh' OR lh.platform LIKE '%OS X%') AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_macos,
    COUNT(IF((lh.platform LIKE 'Linux%' OR lh.platform LIKE 'linux%' OR lh.platform = 'Fedora') AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_linux,
    COUNT(IF(lh.platform = 'ChromeOS' AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30, lh.id, NULL)) AS logins_first30_chromeos,
    COUNT(IF(
        lh.platform NOT LIKE 'Android%'
        AND lh.platform NOT LIKE 'iPhone%'
        AND lh.platform NOT LIKE 'iPad%'
        AND lh.platform <> 'Windows'
        AND lh.platform <> 'Macintosh'
        AND lh.platform NOT LIKE '%OS X%'
        AND lh.platform NOT LIKE 'Linux%'
        AND lh.platform NOT LIKE 'linux%'
        AND lh.platform <> 'Fedora'
        AND lh.platform <> 'ChromeOS'
        AND DATE_DIFF(DATE(lh.created_at), u.signup, DAY) BETWEEN 0 AND 30,
    lh.id, NULL)) AS logins_first30_other,
    COUNT(IF(lh.platform IS NULL AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_unknown,
    COUNT(IF(lh.platform LIKE 'Android%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_android,
    COUNT(IF(lh.platform LIKE 'iPhone%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_iphone,
    COUNT(IF(lh.platform LIKE 'iPad%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_ipad,
    COUNT(IF(lh.platform = 'Windows' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_windows,
    COUNT(IF((lh.platform = 'Macintosh' OR lh.platform LIKE '%OS X%') AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_macos,
    COUNT(IF((lh.platform LIKE 'Linux%' OR lh.platform LIKE 'linux%' OR lh.platform = 'Fedora') AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_linux,
    COUNT(IF(lh.platform = 'ChromeOS' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date, lh.id, NULL)) AS logins_before_first_retail_chromeos,
    COUNT(IF(
        lh.platform NOT LIKE 'Android%'
        AND lh.platform NOT LIKE 'iPhone%'
        AND lh.platform NOT LIKE 'iPad%'
        AND lh.platform <> 'Windows'
        AND lh.platform <> 'Macintosh'
        AND lh.platform NOT LIKE '%OS X%'
        AND lh.platform NOT LIKE 'Linux%'
        AND lh.platform NOT LIKE 'linux%'
        AND lh.platform <> 'Fedora'
        AND lh.platform <> 'ChromeOS'
        AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_retail.order_date,
    lh.id, NULL)) AS logins_before_first_retail_other,
    COUNT(IF(lh.platform IS NULL AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_unknown,
    COUNT(IF(lh.platform LIKE 'Android%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_android,
    COUNT(IF(lh.platform LIKE 'iPhone%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_iphone,
    COUNT(IF(lh.platform LIKE 'iPad%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_ipad,
    COUNT(IF(lh.platform = 'Windows' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_windows,
    COUNT(IF((lh.platform = 'Macintosh' OR lh.platform LIKE '%OS X%') AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_macos,
    COUNT(IF((lh.platform LIKE 'Linux%' OR lh.platform LIKE 'linux%' OR lh.platform = 'Fedora') AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_linux,
    COUNT(IF(lh.platform = 'ChromeOS' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date, lh.id, NULL)) AS logins_before_first_wholesale_chromeos,
    COUNT(IF(
        lh.platform NOT LIKE 'Android%'
        AND lh.platform NOT LIKE 'iPhone%'
        AND lh.platform NOT LIKE 'iPad%'
        AND lh.platform <> 'Windows'
        AND lh.platform <> 'Macintosh'
        AND lh.platform NOT LIKE '%OS X%'
        AND lh.platform NOT LIKE 'Linux%'
        AND lh.platform NOT LIKE 'linux%'
        AND lh.platform <> 'Fedora'
        AND lh.platform <> 'ChromeOS'
        AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_wholesale.order_date,
    lh.id, NULL)) AS logins_before_first_wholesale_other,
    COUNT(IF(lh.platform IS NULL AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_unknown,
    COUNT(IF(lh.platform LIKE 'Android%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_android,
    COUNT(IF(lh.platform LIKE 'iPhone%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_iphone,
    COUNT(IF(lh.platform LIKE 'iPad%' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_ipad,
    COUNT(IF(lh.platform = 'Windows' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_windows,
    COUNT(IF((lh.platform = 'Macintosh' OR lh.platform LIKE '%OS X%') AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_macos,
    COUNT(IF((lh.platform LIKE 'Linux%' OR lh.platform LIKE 'linux%' OR lh.platform = 'Fedora') AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_linux,
    COUNT(IF(lh.platform = 'ChromeOS' AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date, lh.id, NULL)) AS logins_before_first_autoship_chromeos,
    COUNT(IF(
        lh.platform NOT LIKE 'Android%'
        AND lh.platform NOT LIKE 'iPhone%'
        AND lh.platform NOT LIKE 'iPad%'
        AND lh.platform <> 'Windows'
        AND lh.platform <> 'Macintosh'
        AND lh.platform NOT LIKE '%OS X%'
        AND lh.platform NOT LIKE 'Linux%'
        AND lh.platform NOT LIKE 'linux%'
        AND lh.platform <> 'Fedora'
        AND lh.platform <> 'ChromeOS'
        AND lh.created_at BETWEEN DATETIME(u.signup) AND ao.first_autoship.order_date,
    lh.id, NULL)) AS logins_before_first_autoship_other
  FROM pyr_monat_prd.pyr_login_histories lh
  JOIN users u ON lh.user_id = u.user_id
  JOIN alpha_omega ao ON u.tree_user_id = ao.commission_user_id
  GROUP BY u.tree_user_id
)
#-##
SELECT
  om.*,
  ao.* EXCEPT (commission_user_id, first_retail, first_wholesale, first_autoship, last_retail, last_wholesale, last_autoship),
  sea.* EXCEPT (sender_tree_user_id),
  l.* EXCEPT (tree_user_id)
FROM order_metrics om
LEFT JOIN alpha_omega ao ON om.commission_user_id = ao.commission_user_id
LEFT JOIN sent_emails_alpha sea ON om.commission_user_id = sea.sender_tree_user_id
LEFT OUTER JOIN logins l ON om.commission_user_id = l.tree_user_id

