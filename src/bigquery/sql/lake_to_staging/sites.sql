INSERT INTO staging.flat_site_visitors (
  icentris_client,
  site_id,
  user_id,
  tree_user_id,
  visitor_id,
  last_visit_date,
  visit_count,
  ipaddress,
  browser_agent,
  created_at,
  site_template_id,
  active,
  third_party_tracking_company,
  tracking_code,
  owner_name,
  email,
  story,
  avatar_file_name
)
SELECT
    'monat' AS icentris_client,
    s.id AS site_id,
    u.id AS user_id,
    u.tree_user_id,
    sv.visitor_id,
    sv.last_visit_date,
    sv.visit_count,
    sv.ipaddress,
    sv.browser_agent,
    sv.created_at,
    NULL,
    NULL,
    atc.third_party_tracking_company,
    atc.tracking_code,
    NULL,
    NULL,
    NULL,
    NULL
FROM pyr_monat_prd.pyr_sites s
LEFT OUTER JOIN pyr_monat_prd.pyr_site_visitors sv ON s.id = sv.site_id
INNER JOIN pyr_monat_prd.users u ON CAST(s.user_id AS INT64) = u.id
LEFT OUTER JOIN pyr_monat_prd.pyr_sites_analytics_tracking_codes atc ON u.id = atc.user_id
