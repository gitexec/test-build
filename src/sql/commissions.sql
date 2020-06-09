##-# commissions
CREATE TABLE IF NOT EXISTS `data-science`.commissions (
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

REPLACE INTO `data-science`.commissions
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
INNER JOIN tree_commissions tc ON u.tree_user_id = tc.tree_user_id
INNER JOIN tree_commission_runs tcr ON tc.commission_run_id = tcr.id
INNER JOIN tree_periods tp ON tcr.period_id = tp.id
INNER JOIN tree_period_types tpt ON tp.period_type_id = tpt.id;
#-##
