##-# data-science schema
CREATE SCHEMA IF NOT EXISTS `data-science`
  DEFAULT CHARACTER SET = utf8mb4
  DEFAULT COLLATE = utf8mb4_unicode_ci;

USE `data-science`;
#-##

DELIMITER //


##-# data_science
DROP PROCEDURE IF EXISTS data_science//

CREATE PROCEDURE data_science ()
BEGIN

CALL users();

CALL commissions();

CALL orders();

CALL logins();

#CALL `sponsored-signups`(client);

#CALL `sponsored-orders`(client);

CALL `product-reviews`();

END//
#-##

##-# users
CREATE TABLE IF NOT EXISTS `data-science`.users (
  `tree_user_id` INT UNSIGNED NOT NULL,
  `user_id` INT UNSIGNED NOT NULL,
  `sponsor_id` INT UNSIGNED NULL,
  `parent_id` INT UNSIGNED NULL,
  `type` VARCHAR(50) NULL,
  first_name VARCHAR(255) NULL,
  last_name VARCHAR(255) NULL,
  email VARCHAR(255) NULL,
  city VARCHAR(255) NULL,
  state VARCHAR(100) NULL,
  zip VARCHAR(20) NULL,
  country VARCHAr(100) NULL,
  gender ENUM('Male', 'Female', 'Unknown') NULL,
  age TINYINT UNSIGNED,
  signup_date DATETIME,
  `status` VARCHAR(50) NULL,
  `modified` DATETIME,
  PRIMARY KEY (`tree_user_id`)
)ENGINE=Innodb;


REPLACE INTO `data-science`.`users`
SELECT
  tu.id AS `tree_user_id`,
  u.id AS `user_id`,
  tu.sponsor_id,
  tu.parent_id,
  tt.description,
  tu.first_name,
  tu.last_name,
  tu.email,
  city,
  state,
  zip,
  country,
  gender,
  TIMESTAMPDIFF(YEAR,birth_date,NOW()) AS age,
  tu.date1 AS `signup_date`,
  tus.description AS `status`,
  tu.updated_date AS `last_modified`
FROM tree_users tu
LEFT OUTER JOIN users u ON tu.id = u.tree_user_id
INNER JOIN tree_user_types tt ON tu.user_type_id = tt.id
INNER JOIN tree_user_statuses tus ON tu.user_status_id = tus.id;
#-##

##-# commissions
DROP PROCEDURE IF EXISTS commissions//

CREATE PROCEDURE commissions ()
BEGIN

CREATE TABLE IF NOT EXISTS commissions (
  tree_user_id INT UNSIGNED NOT NULL,
  run_id INT UNSIGNED NOT NULL,
  run_date DATETIME,
  period_id INT UNSIGNED NOT NULL,
  period_type_id INT UNSIGNED NOT NULL,
  period VARCHAR(50) NULL,
  period_type VARCHAR(50) NULL,
  currency_code VARCHAR(10),
  earnings DECIMAL(19,4),
  PRIMARY KEY (tree_user_id, run_id, period_id),
  INDEX(period_type)
);

REPLACE INTO commissions
SELECT
  u.tree_user_id,
  tcr.id,
  tcr.run_date,
  tp.id,
  tpt.id,
  tp.description,
  tpt.description,
  tc.currency_code,
  tc.earnings
FROM users u
INNER JOIN `pyr-idlife-prod`.tree_commissions tc ON u.tree_user_id = tc.tree_user_id
INNER JOIN `pyr-idlife-prod`.tree_commission_runs tcr ON tc.commission_run_id = tcr.id
INNER JOIN `pyr-idlife-prod`.tree_periods tp ON tcr.period_id = tp.id
INNER JOIN `pyr-idlife-prod`.tree_period_types tpt ON tp.period_type_id = tpt.id;

END //
#-##

##-# orders
CREATE TABLE IF NOT EXISTS `data-science`.`orders` (
  `order_id` INT UNSIGNED NOT NULL,
  `order_item_id` int(11) NOT NULL,
  `tree_user_id` INT UNSIGNED NOT NULL,
  `type` VARCHAR(50) NOT NULL,
  `status` VARCHAR(50) NOT NULL,
  `is_autoship` bool DEFAULT 0,
  `order_date` datetime DEFAULT NULL,
  `currency_code` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `total` decimal(19,4) DEFAULT '0.0000',
  `sub_total` decimal(19,4) DEFAULT '0.0000',
  `tax_total` decimal(19,4) DEFAULT '0.0000',
  `shipping_total` decimal(19,4) DEFAULT '0.0000',
  `bv_total` decimal(19,4) DEFAULT '0.0000',
  `cv_total` decimal(19,4) DEFAULT '0.0000',
  `discount_total` decimal(19,4) DEFAULT '0.0000',
  `discount_code` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `timezone` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `shipped_date` datetime DEFAULT NULL,
  `order_shipping_city` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `order_shipping_state` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `order_shipping_zip` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `order_shipping_country` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `order_shipping_county` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `order_tracking_number` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `modified_date` datetime DEFAULT NULL,
  `client_order_id` int(11) DEFAULT NULL,
  `client_user_id` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `product_code` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `product_description` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `quantity` int(11) DEFAULT NULL,
  `price_each` decimal(19,4) DEFAULT '0.0000',
  `price_total` decimal(19,4) DEFAULT '0.0000',
  `weight_each` decimal(19,4) DEFAULT '0.0000',
  `weight_total` decimal(19,4) DEFAULT '0.0000',
  `tax` decimal(19,4) DEFAULT '0.0000',
  `bv` decimal(19,4) DEFAULT '0.0000',
  `bv_each` decimal(19,4) DEFAULT '0.0000',
  `cv_each` decimal(19,4) DEFAULT '0.0000',
  `cv` decimal(19,4) DEFAULT '0.0000',
  `parent_product_id` int(11) DEFAULT NULL,
  `item_tracking_number` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `item_shipping_city` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `item_shipping_state` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `item_shipping_zip` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `item_shipping_county` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `item_shipping_country` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (order_id, order_item_id)
) ENGINE=Innodb;

REPLACE INTO `data-science`.`orders`
SELECT
  o.id,
  i.order_item_id,
  o.tree_user_id,
  t.description AS `type`,
  s.description AS `status`,
  IF(o.`autoship_template_id` > 0, 1, 0),
  o.`order_date`,
  o.`currency_code`,
  o.`total`,
  o.`sub_total`,
  o.`tax_total`,
  o.`shipping_total`,
  o.`bv_total`,
  o.`cv_total`,
  o.`discount_total`,
  o.`discount_code`,
  o.`timezone`,
  o.`shipped_date`,
  o.`shipping_city`,
  o.`shipping_state`,
  o.`shipping_zip`,
  o.`shipping_country`,
  o.`shipping_county`,
  o.`tracking_number`,
  o.`created_date`,
  o.`modified_date`,
  o.`client_order_id`,
  o.`client_user_id`,
  i.`product_id`,
  i.`product_code`,
  i.`product_description`,
  i.`quantity`,
  i.`price_each`,
  i.`price_total`,
  i.`weight_each`,
  i.`weight_total`,
  i.`tax`,
  i.`bv`,
  i.`bv_each`,
  i.`cv_each`,
  i.`cv`,
  i.`parent_product_id`,
  i.`tracking_number`,
  i.`shipping_city`,
  i.`shipping_state`,
  i.`shipping_zip`,
  i.`shipping_county`,
  i.`shipping_country`
FROM tree_orders o
INNER JOIN tree_order_items i ON o.id = i.order_id
INNER JOIN tree_order_types t ON o.order_type_id = t.id
INNER JOIN tree_order_statuses s ON o.order_status_id = s.id;
#-##

##-# logins
DROP PROCEDURE IF EXISTS `logins`//

CREATE PROCEDURE logins ()
BEGIN

CREATE TABLE IF NOT EXISTS `logins` (
  `tree_user_id` INT(11) UNSIGNED NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `user_agent` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `browser` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `browser_version` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `platform` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `ip_address` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY (`tree_user_id`),
  KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

REPLACE INTO `logins`
SELECT
  u.tree_user_id,
  lh.`user_id`,
  lh.`created_at`,
  lh.`updated_at`,
  lh.`user_agent`,
  lh.`browser`,
  lh.`browser_version`,
  lh.`platform`,
  lh.`ip_address`
FROM users u
INNER JOIN `pyr-idlife-prod`.`pyr_login_histories` lh ON u.user_id = lh.user_id
WHERE lh.proxied_by_id IS NULL;

END//
#-##

##-# sponsored signups
DROP PROCEDURE IF EXISTS `sponsored-signups`//

CREATE PROCEDURE `sponsored-signups` (IN `client` VARCHAR(100))
BEGIN

DECLARE q VARCHAR(16000) DEFAULT "
REPLACE INTO `sponsored-signups`
SELECT
  p.client,
  p.id,
  COUNT(IF(TIMESTAMPDIFF(DAY, p.signup_date, c.signup_date) < 8, c.id, NULL)),
  COUNT(c.id)
FROM users p
INNER JOIN %schema%.tree_sponsors s ON p.id = s.upline_id
INNER JOIN users c
  ON s.tree_user_id = c.id
  AND p.client = c.client
  AND TIMESTAMPDIFF(DAY, p.signup_date, c.signup_date) < 31
WHERE p.client = ?
GROUP BY p.client, p.id;
";

CREATE TABLE IF NOT EXISTS `sponsored-signups` (
  `client` ENUM('idlife', 'saba', 'trurise', 'liveultimate', 'mannatech', 'retrobeauty', 'tupperware', 'monat', 'worldventures', 'avon', 'thirtyonegifts') NOT NULL,
  `user_id` INT UNSIGNED NOT NULL,
  `signups_7_days` INT UNSIGNED NULL,
  `signups_30_days` INT UNSIGNED NULL,
  PRIMARY KEY (`client`, `user_id`)
);

SELECT `client` INTO @client;
CALL `schema`(client, @schema);
CALL `stmt` (@schema, q, @q);

PREPARE stmt FROM @q;

EXECUTE stmt USING @client;

DEALLOCATE PREPARE stmt;

END//
#-##

##-# sponsored orders
DROP PROCEDURE IF EXISTS `sponsored-orders`//

CREATE PROCEDURE `sponsored-orders` (IN `client` VARCHAR(100))
BEGIN

DECLARE q VARCHAR(16000) DEFAULT '
REPLACE INTO `sponsored-orders`
SELECT
  p.client,
  p.id,
  MIN(@secs := TIMESTAMPDIFF(SECOND, p.signup_date, o.order_date)),
  COUNT(IF(@secs < 0, o.id, NULL)),
  SUM(IF(@secs < 0, o.total, NULL)),
  SUM(IF(@secs < 0, o.bv_total, NULL)),
  SUM(IF(@secs < 0, o.cv_total, NULL)),
  COUNT(IF(@secs > -1 AND @days := TIMESTAMPDIFF(DAY, p.signup_date, o.order_date) < 8, o.id, NULL)),
  SUM(IF(@secs > -1 AND @days < 8, o.total, NULL)),
  SUM(IF(@secs > -1 AND @days < 8, o.bv_total, NULL)),
  SUM(IF(@secs > -1 AND @days < 8, o.cv_total, NULL)),
  COUNT(IF(@secs > -1, o.id, NULL)),
  SUM(IF(@secs > -1, o.total, NULL)),
  SUM(IF(@secs > -1, o.bv_total, NULL)),
  SUM(IF(@secs > -1, o.cv_total, NULL))
FROM users p
INNER JOIN %schema%.tree_sponsors s ON p.id = s.upline_id
INNER JOIN users c
  ON s.tree_user_id = c.id
  AND p.client = c.client
INNER JOIN %schema%.tree_orders o
  ON c.id = o.tree_user_id
  AND TIMESTAMPDIFF(DAY, p.signup_date, o.order_date) < 31
WHERE p.client = ?
GROUP BY p.client, p.id;
';

CREATE TABLE IF NOT EXISTS `sponsored-orders` (
  `client` ENUM('idlife', 'saba', 'trurise', 'liveultimate', 'mannatech', 'retrobeauty', 'tupperware', 'monat', 'worldventures', 'avon', 'thirtyonegifts') NOT NULL,
  user_id INT UNSIGNED NOT NULL,
  sponsored_seconds_from_signup_to_first_order INT NULL,
  sponsored_orders_before_signup INT UNSIGNED NULL,
  sponsored_total_before_signup DECIMAL(19,4) NULL,
  sponsored_personal_volume_before_signup DECIMAL(19,4) NULL,
  sponsored_commissionable_volume_before_signup DECIMAL(19,4) NULL,
  sponsored_orders_first_7_after_signup INT UNSIGNED NULL,
  sponsored_total_first_7_after_signup DECIMAL(19,4) NULL,
  sponsored_personal_volume_first_7_after_signup DECIMAL(19,4) NULL,
  sponsored_commissionable_volume_first_7_after_signup DECIMAL(19,4) NULL,
  sponsored_orders_first_30_after_signup INT UNSIGNED NULL,
  sponsored_total_first_30_after_signup DECIMAL(19,4) NULL,
  sponsored_personal_volume_first_30_after_signup DECIMAL(19,4) NULL,
  sponsored_commissionable_volume_first_30_after_signup DECIMAL(19,4) NULL,
  PRIMARY KEY (`client`, user_id)
);

SELECT `client` INTO @client;
CALL `schema`(client, @schema);
CALL `stmt` (@schema, q, @q);

PREPARE stmt FROM @q;

EXECUTE stmt USING @client;

DEALLOCATE PREPARE stmt;

END//
#-##

##-# product-reviews
DROP PROCEDURE IF EXISTS `product-reviews`//

CREATE PROCEDURE `product-reviews` ()
BEGIN
CREATE TABLE IF NOT EXISTS `product-reviews` (
  `product_code` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `product_name` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `product_description` text COLLATE utf8_unicode_ci,
  `product_short_description` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `product_available_on` datetime DEFAULT NULL,
  `product_discontinue_on` datetime DEFAULT NULL,
  `product_slug` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `product_avg_rating` decimal(7,5) NOT NULL DEFAULT '0.00000',
  `product_reviews_count` int(11) NOT NULL DEFAULT '0',
  `review_name` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `location` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rating` int(11) DEFAULT NULL,
  `title` text COLLATE utf8_unicode_ci,
  `review` text COLLATE utf8_unicode_ci,
  `approved` tinyint(1) DEFAULT '0',
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `tree_user_id` int(11) DEFAULT NULL,
  `ip_address` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `locale` varchar(255) COLLATE utf8_unicode_ci DEFAULT 'en',
  `show_identifier` tinyint(1) DEFAULT '1',
  KEY (`user_id`),
  KEY (`tree_user_id`)
) ENGINE=InnoDB;

REPLACE INTO `product-reviews`
SELECT
  v.sku AS `product_code`,
  p.name AS `product_name`,
  p.description AS `product_description`,
  p.short_description AS `product_short_description`,
  p.available_on AS `product_available_on`,
  p.discontinue_on AS `product_discontinue_on`,
  p.slug AS `product_slug`,
  p.avg_rating AS `product_avg_rating`,
  p.reviews_count AS `product_reviews_count`,
  r.`name` AS `review_name`,
  r.`location`,
  r.`rating`,
  r.`title`,
  r.`review`,
  r.`approved`,
  r.`created_at`,
  r.`updated_at`,
  r.`user_id`,
  u.`tree_user_id`,
  r.`ip_address`,
  r.`locale`,
  r.`show_identifier`
FROM `pyr-idlife-prod`.spree_reviews r
INNER JOIN `pyr-idlife-prod`.users u ON r.user_id = u.id
INNER JOIN `pyr-idlife-prod`.spree_products p ON r.product_id = p.id
INNER JOIN `pyr-idlife-prod`.spree_variants v ON p.id = v.product_id;

END
//
#-##

SELECT GROUP_CONCAT(COLUMN_NAME)
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'data-science'
AND `TABLE_NAME` IN (
  'users',
  'summary_commissions',
  'orders',
  'logins',
  'sponsored-signups',
  'sponsored-orders',
  'product-reviews'
)
AND CASE
  WHEN TABLE_NAME = 'users' THEN 1=1
  ELSE COLUMN_NAME <> 'client'
END
AND COLUMN_NAME <> 'user_id'
ORDER BY CASE
  WHEN TABLE_NAME = 'users' THEN 1
  WHEN TABLE_NAME = 'summary_commissions' THEN 2
  WHEN TABLE_NAME = 'orders' THEN 3
  WHEN TABLE_NAME = 'logins' THEN 4
  WHEN TABLE_NAME = 'sponsored-signups' THEN 5
  WHEN TABLE_NAME = 'sponsored-orders' THEN 6
  WHEN TABLE_NAME = 'product-reviews' THEN 7
  END,
CASE
  WHEN COLUMN_NAME = 'id' THEN 1
  WHEN COLUMN_NAME = 'client' THEN 2
  ELSE COLUMN_NAME
END;
