CREATE TABLE IF NOT EXISTS `data-science`.`tasks` (
    id INT UNSIGNED NOT NULL PRIMARY KEY,
    task_title VARCHAR(255) NULL,
    description TEXT NULL,
    due_date DATETIME,
    user_id INT(11) UNSIGNED,
    tree_user_id INT UNSIGNED NULL,
    `from_system` BOOL DEFAULT 0,
    priority TINYINT UNSIGNED DEFAULT NULL,
    created_at DATETIME,
    completed_at DATETIME DEFAULT NULL
) ENGINE=Innodb;


REPLACE INTO `data-science`.`tasks`
SELECT
  t.id,
  task_title,
  description,
  due_date,
  user_id,
  u.tree_user_id,
  IF(source = 'system',1,0) AS `from_system`,
  priority,
  t.created_at,
  t.completed_at
FROM `pyr_user_tasks` t
INNER JOIN users u ON t.user_id = u.id
WHERE task_title NOT LIKE '%test%';
