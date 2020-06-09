DROP TABLE IF EXISTS `data-science`.`site_visitors`;

CREATE TABLE `data-science`.`site_visitors` AS
SELECT
  s.id AS site_id,
  u.id AS user_id,
  u.tree_user_id,
  atc.third_party_tracking_company,
  atc.tracking_code,
  sv.visitor_id,
  sv.last_visit_date,
  sv.visit_count,
  sv.ipaddress,
  sv.browser_agent,
  sv.created_at
FROM pyr_sites s
LEFT OUTER JOIN pyr_site_visitors sv ON s.id = sv.site_id
INNER JOIN users u ON s.user_id = u.id
LEFT OUTER JOIN pyr_sites_analytics_tracking_codes atc ON u.id = atc.user_id