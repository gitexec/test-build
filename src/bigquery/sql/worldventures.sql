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
        o.order_date
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
SELECT
    membership_type,
    COUNT(*) AS cnt,
    COUNT(DISTINCT tree_user_id) AS distinct_users,
    SUM(total) AS total,
    SUM(total) / COUNT(DISTINCT tree_user_id) AS avg_tot_per_user,
FROM orders
WHERE product_type = 'subscription'
GROUP BY membership_type
