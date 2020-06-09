WITH
##-# users
users AS (
    SELECT
    u.client_partition_id,
    u.tree_user_id,
    u.sponsor_id,
    u.type,
    u.client_type,
    DATE(u.coded) AS coded
    FROM vibe_staging.users u
    INNER JOIN pii.users pu
    ON u.tree_user_id = pu.tree_user_id
    AND u.client_partition_id = 2
    AND pu.country = 'US'
    WHERE u.coded >= '2017-01-01'
    AND u.client_type <> 'Employee'
),
#-##
##-# orders
orders AS (
    SELECT
    o.tree_user_id,
    o.order_id,
    p.product_id,
    IF(LOWER(p.product_description) LIKE '%college%', TRUE, FALSE) AS is_college,
        CASE
    WHEN LOWER(p.product_description) LIKE '%reactivation%' THEN 'fee'
    WHEN LOWER(p.product_description) LIKE '%m%nth%' THEN 'subscription'
    WHEN LOWER(p.product_description) LIKE '%tax%' THEN 'tax'
    WHEN LOWER(p.product_description) LIKE '%fee%' THEN 'fee'
    WHEN LOWER(p.product_description) LIKE '%initial%free%' THEN 'discount'
    WHEN LOWER(p.product_description) LIKE '%get4discount%' THEN 'discount'
    WHEN LOWER(p.product_description) LIKE '%activation%' THEN 'fee'
    WHEN LOWER(p.product_description) LIKE '%event%' THEN 'event'
    WHEN LOWER(p.product_description) LIKE '%boot%camp%' THEN 'event'
    WHEN LOWER(p.product_description) LIKE '%momentum%' THEN 'event'
    WHEN LOWER(p.product_description) LIKE '%view%' THEN 'event'
    WHEN LOWER(p.product_description) LIKE '%acceleration%' THEN 'event'
    WHEN LOWER(p.product_description) LIKE '%pick your product%' THEN 'product'
    ELSE p.product_description
END AS product_type,
CASE
    WHEN LOWER(p.product_description) LIKE '%rbs%' THEN 'rep'
    WHEN LOWER(p.product_description) LIKE '%dtp%' THEN 'dream trips platinum'
    WHEN LOWER(p.product_description) LIKE '%dtg%' OR LOWER(p.product_description) LIKE '%dt%gold%' THEN 'dream trips gold'
    WHEN LOWER(p.product_description) LIKE '%dts%' THEN 'dream trips silver'
    WHEN LOWER(p.product_description) LIKE '%dtu%' THEN 'dream trips u'
    WHEN LOWER(p.product_description) LIKE '%wva%' THEN 'worldventures a'
    ELSE 'none'
END AS membership_type,
p.price_each,
p.quantity,
p.price_total AS total,
p.product_description AS client_description
FROM vibe_staging.orders o, UNNEST(products) p
JOIN users u
ON o.tree_user_id = u.tree_user_id
AND o.client_partition_id = u.client_partition_id
AND o.status = 'Active'
),
#-##
##-# firsts
firsts AS (
    SELECT
    c.sponsor_id,
    ARRAY_AGG(STRUCT(
            c.tree_user_id,
            c.coded,
            DATE_DIFF(c.coded, p.coded, DAY) AS days_to_signup,
        CASE
            WHEN DATE_DIFF(c.coded, p.coded, DAY) = 0 THEN '0'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 1 AND 15 THEN '1-15'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 16 AND 30 THEN '16-30'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 31 AND 90 THEN '31-90'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 91 AND 180 THEN '91-180'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 181 AND 365 THEN '181-365'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 366 AND 730 THEN '366-730'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) > 730 THEN '> 730'
        END AS bucket,
        CASE
            WHEN DATE_DIFF(c.coded, p.coded, DAY) = 0 THEN '1'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 1 AND 15 THEN '2'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 16 AND 30 THEN '3'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 31 AND 90 THEN '4'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 91 AND 180 THEN '5'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 181 AND 365 THEN '6'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 366 AND 730 THEN '7'
            WHEN DATE_DIFF(c.coded, p.coded, DAY) > 730 THEN '8'
        END AS bucket_order
    ) ORDER BY c.coded ASC LIMIT 1)[OFFSET (0)] AS child
FROM users p
JOIN users c ON p.tree_user_id = c.sponsor_id
WHERE DATE_DIFF(c.coded, p.coded, DAY) > 0
GROUP BY c.sponsor_id
),
#-##
##-# sponsored
sponsored AS (
    SELECT
    p.tree_user_id,
    COUNT(c.tree_user_id) AS cnt,
    COUNT(IF(DATE_DIFF(c.coded, p.coded, DAY) BETWEEN 0 AND 30, c.tree_user_id, NULL)) AS first_30_cnt,
    AVG(DATE_DIFF(c.coded, p.coded, DAY)) AS avg_days_to_signup,
    MAX(DATE_DIFF(c.coded, p.coded, DAY)) AS max_days_to_signup
    FROM users p
    JOIN users c ON p.tree_user_id = c.sponsor_id
    WHERE DATE_DIFF(c.coded, p.coded, DAY) > 0
    GROUP BY p.tree_user_id
),
#-##
##-# order_aggs
order_aggs AS (
    SELECT
    o.tree_user_id,
    COUNT(IF(o.product_type = 'fee' AND (o.membership_type <> 'rep'), o.order_id, NULL)) AS vaca_fees_cnt,
    SUM(IF(o.product_type = 'fee' AND (o.membership_type <> 'rep'), o.total, 0)) AS vaca_fees_tot,
    COUNT(IF(o.product_type = 'discount' AND (o.membership_type <> 'rep'), o.order_id, NULL)) AS vaca_discounts_cnt,
    SUM(IF(o.product_type = 'discount' AND (o.membership_type <> 'rep'), o.total, 0)) AS vaca_discounts_tot,
    COUNT(IF(o.product_type = 'subscription' AND (o.membership_type <> 'rep'), o.order_id, NULL)) AS vaca_subs_cnt,
    SUM(IF(o.product_type = 'subscription' AND (o.membership_type <> 'rep'), o.total, 0)) AS vaca_subs_tot,
    COUNT(IF(o.product_type = 'fee' AND (o.membership_type = 'rep'), o.order_id, NULL)) AS rep_fees_cnt,
    SUM(IF(o.product_type = 'fee' AND (o.membership_type = 'rep'), o.total, 0)) AS rep_fees_tot,
    COUNT(IF(o.product_type = 'discount' AND (o.membership_type = 'rep'), o.order_id, NULL)) AS rep_discounts_cnt,
    SUM(IF(o.product_type = 'discount' AND (o.membership_type = 'rep'), o.total, 0)) AS rep_discounts_tot,
    COUNT(IF(o.product_type = 'subscription' AND (o.membership_type = 'rep'), o.order_id, NULL)) AS rep_subs_cnt,
    SUM(IF(o.product_type = 'subscription' AND (o.membership_type = 'rep'), o.total, 0)) AS rep_subs_tot,
    COUNT(IF(o.product_type = 'event', o.order_id, NULL)) AS event_cnt,
    SUM(IF(o.product_type = 'event', o.total, 0)) AS event_tot,
    COUNT(IF(o.product_type = 'product', o.order_id, NULL)) AS product_cnt,
    SUM(IF(o.product_type = 'product', o.total, 0)) AS product_tot,
    SUM(o.total) AS tot
    FROM orders o
    GROUP BY o.tree_user_id
)
#-##
SELECT
    f.child.bucket,
    COUNT(u.tree_user_id) AS cnt,
    SUM(s.cnt) AS sponsored_cnt,
    SUM(s.first_30_cnt) AS sponsored_first_30_cnt,
    AVG(avg_days_to_signup) AS avg_days_to_signup,
    AVG(f.child.days_to_signup) AS avg_min_days_to_signup,
    AVG(max_days_to_signup) AS avg_max_days_to_signup,
    AVG(max_days_to_signup - f.child.days_to_signup) AS avg_days_active,
    SUM(IFNULL(vaca_fees_cnt,0)) AS vaca_fees_cnt,
    SUM(IFNULL(vaca_fees_tot,0)) AS vaca_fees_tot,
    SUM(IFNULL(vaca_discounts_cnt,0)) AS vaca_discounts_cnt,
    SUM(IFNULL(vaca_discounts_tot,0)) AS vaca_discounts_tot,
    SUM(IFNULL(vaca_subs_cnt,0)) AS vaca_subs_cnt,
    SUM(IFNULL(vaca_subs_cnt,0)) / COUNT(u.tree_user_id) AS vaca_subs_per_user,
    SUM(IFNULL(vaca_subs_tot,0)) AS vaca_subs_tot,
    SUM(IFNULL(vaca_subs_tot,0)) / COUNT(u.tree_user_id) AS vaca_subs_tot_per_user,
    SUM(IFNULL(rep_fees_cnt,0)) AS rep_fees_cnt,
    SUM(IFNULL(rep_fees_tot,0)) AS rep_fees_tot,
    SUM(IFNULL(rep_discounts_cnt,0)) AS rep_discounts_cnt,
    SUM(IFNULL(rep_discounts_tot,0)) AS rep_discounts_tot,
    SUM(IFNULL(rep_subs_cnt,0)) AS rep_subs_cnt,
    SUM(IFNULL(rep_subs_cnt,0)) / COUNT(u.tree_user_id) AS reps_subs_per_user,
    SUM(IFNULL(rep_subs_tot,0)) AS rep_subs_tot,
    SUM(IFNULL(rep_subs_tot,0)) / COUNT(u.tree_user_id) AS reps_subs__tot_per_user,
    SUM(IFNULL(event_cnt,0)) AS event_cnt,
    SUM(IFNULL(event_tot,0)) AS event_tot,
    SUM(IFNULL(product_cnt,0)) AS product_cnt,
    SUM(IFNULL(product_tot,0)) AS product_tot,
    SUM(IFNULL(tot,0)) AS tot,
    SUM(IFNULL(tot,0)) / COUNT(u.tree_user_id) AS avg_rev_per_user
FROM users u
LEFT JOIN firsts f ON u.tree_user_id = f.sponsor_id
LEFT JOIN sponsored s ON u.tree_user_id = s.tree_user_id
LEFT JOIN order_aggs oa ON u.tree_user_id = oa.tree_user_id
WHERE u.type = 'Distributor'
GROUP BY f.child.bucket, f.child.bucket_order
ORDER BY f.child.bucket_order
