##-# users
CREATE TABLE IF NOT EXISTS `data-science`.users (
  `tree_user_id` INT UNSIGNED NOT NULL,
  `user_id` INT UNSIGNED NOT NULL,
  `sponsor_id` INT UNSIGNED NULL,
  `parent_id` INT UNSIGNED NULL,
  `type` VARCHAR(50) NULL,
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


REPLACE INTO `users`
SELECT
  tu.id AS `tree_user_id`,
  u.id AS `user_id`,
  tu.sponsor_id,
  tu.parent_id,
  tt.description,
  city,
  state,
  zip,
  country,
  gender,
  TIMESTAMPDIFF(YEAR,birth_date,NOW()) AS age,
  tu.date1 AS `signup_date`,
  tus.description AS `status`,
  tu.updated_date AS `last_modified`
FROM `pyr-idlife-prod`.tree_users tu
LEFT OUTER JOIN `pyr-idlife-prod`.users u ON tu.id = u.tree_user_id
INNER JOIN `pyr-idlife-prod`.tree_user_types tt ON tu.user_type_id = tt.id
INNER JOIN `pyr-idlife-prod`.tree_user_statuses tus ON tu.user_status_id = tus.id;
#-##
