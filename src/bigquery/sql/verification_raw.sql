SELECT *
FROM (
  SELECT 'pyr_contact_emails' AS tbl, COUNT(*) AS cnt, NULL AS `min`, NULL AS `max` FROM pyr_worldventures_prd.pyr_contact_emails
  UNION ALL
  SELECT 'pyr_contact_phone_numbers', COUNT(*), NULL AS `min`, NULL AS `max` FROM pyr_worldventures_prd.pyr_contact_phone_numbers
  UNION ALL
  SELECT 'pyr_contact_categories', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_contact_categories
  UNION ALL
  SELECT 'pyr_contacts_contact_categories', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_contacts_contact_categories
  UNION ALL
  SELECT 'pyr_contacts', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_contacts
  UNION ALL
  SELECT 'pyr_login_histories', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_login_histories
  UNION ALL
  SELECT 'pyr_messages', COUNT(*), MIN(sent_date), MAX(sent_date) FROM pyr_worldventures_prd.pyr_messages
  UNION ALL
  SELECT 'pyr_message_recipients', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_message_recipients
  UNION ALL
  SELECT 'pyr_resources', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_resources
  UNION ALL
  SELECT 'pyr_resource_assets', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_resource_assets
  UNION ALL
  SELECT 'pyr_links', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_links
  UNION ALL
  SELECT 'pyr_events', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_events
  UNION ALL
  SELECT 'pyr_ecards', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_ecards
  UNION ALL
  SELECT 'pyr_resources_view_counts', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_resources_view_counts
  UNION ALL
  SELECT 'pyr_asset_shares', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_asset_shares
  UNION ALL
  SELECT 'pyr_rank_definitions', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_rank_definitions
  UNION ALL
  SELECT 'pyr_resource_categories', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_resource_categories
  UNION ALL
  SELECT 'pyr_resources_categories', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_resources_categories
  UNION ALL
  SELECT 'pyr_site_visitors', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_site_visitors
  UNION ALL
  SELECT 'pyr_user_tasks', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.pyr_user_tasks
  UNION ALL
  SELECT 'spree_products', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.spree_products
  UNION ALL
  SELECT 'spree_variants', COUNT(*), MIN(updated_at), MAX(updated_at) FROM pyr_worldventures_prd.spree_variants
  UNION ALL
  SELECT 'spree_reviews', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.spree_reviews
  UNION ALL
  SELECT 'tree_bonuses', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.tree_bonuses
  UNION ALL
  SELECT 'tree_commissions', COUNT(*), MIN(created_date), MAX(created_date) FROM pyr_worldventures_prd.tree_commissions
  UNION ALL
  SELECT 'tree_commission_bonuses', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.tree_commission_bonuses
  UNION ALL
  SELECT 'tree_commission_details', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.tree_commission_details
  UNION ALL
  SELECT 'tree_commission_runs', COUNT(*), MIN(run_date), MAX(run_date) FROM pyr_worldventures_prd.tree_commission_runs
  UNION ALL
  SELECT 'tree_order_types' AS tbl, COUNT(*) AS cnt, NULL AS `min`, NULL AS `max` FROM pyr_worldventures_prd.tree_order_types
  UNION ALL
  SELECT 'tree_order_statuses' AS tbl, COUNT(*) AS cnt, NULL AS `min`, NULL AS `max` FROM pyr_worldventures_prd.tree_order_statuses
  UNION ALL
  SELECT 'tree_orders', COUNT(*), MIN(order_date), MAX(order_date) FROM pyr_worldventures_prd.tree_orders
  UNION ALL
  SELECT 'tree_order_items', COUNT(*), NULL, NULL FROM pyr_worldventures_prd.tree_order_items
  UNION ALL
  SELECT 'tree_periods', COUNT(*), MIN(start_date), MAX(start_date) FROM pyr_worldventures_prd.tree_periods
  UNION ALL
  SELECT 'tree_user_types' AS tbl, COUNT(*) AS cnt, NULL AS `min`, NULL AS `max` FROM pyr_worldventures_prd.tree_user_types
  UNION ALL
  SELECT 'tree_user_statuses' AS tbl, COUNT(*) AS cnt, NULL AS `min`, NULL AS `max` FROM pyr_worldventures_prd.tree_user_statuses
  UNION ALL
  SELECT 'tree_users' AS tbl, COUNT(*) AS cnt, MIN(date1) AS `min`, MAX(date1) AS `max` FROM pyr_worldventures_prd.tree_users
  UNION ALL
  SELECT 'users', COUNT(*), MIN(created_at), MAX(created_at) FROM pyr_worldventures_prd.users

) t
ORDER BY tbl ASC;
