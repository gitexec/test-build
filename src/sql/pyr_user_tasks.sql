SELECT
  id,
  task_title,
  description,
  due_date,
  user_id,
  IF(source = 'system',1,0) AS `from_system`,
  priority,
  created_at,
  completed_at
FROM `pyr_user_tasks`
WHERE task_title NOT LIKE '%test%';
