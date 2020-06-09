/*
SELECT asset_type, containable_type, channel_type, count(*)
FROM pyr_worldventures_prd.pyr_links l
group by asset_type, containable_type, channel_type
*/


SELECT
  l.id AS link_id,
  l.user_id AS link_sharer_id,
  l.created_at,
  l.asset_id,
  l.asset_type,
  STRUCT(
    STRUCT(
      res.id AS resource_id,
      res.title AS title,
      res.created_at AS created,
      res.updated_at AS resource_updated,
      res.expiry_date AS resource_expires
    ) AS resource,
    ra.title AS title,
    ra.file_type AS file_type,
    ra.file_name AS file_name,
    ra.file_description AS file_description,
    CAST(ra.available_email AS BOOL) AS can_email,
    CAST(ra.allow_download AS BOOL) AS allow_download,
    ra.likes_count AS likes_count,
    ra.shares_count AS shares_count,
    ra.abuse_reports_count AS abuse_reports_count,
    ra.views_count AS views_count,
    ra.downloads_count AS downloads_count,
    ra.created_at AS created,
    ra.updated_at AS updated
  ) AS resource_asset,
  
  l.containable_id,
  l.containable_type,
  l.channel_id,
  l.channel_type,
FROM pyr_monat_prd.pyr_links l
LEFT OUTER JOIN pyr_monat_prd.pyr_resource_assets ra
  ON l.asset_id = ra.id
  AND l.asset_type = 'PyrCore::ResourceAsset'
LEFT OUTER JOIN pyr_monat_prd.pyr_resources res
  ON ra.resource_id = res.id
LEFT OUTER JOIN pyr_monat_prd.pyr_ecards ec
  ON l.asset_id = ec.id
  AND l.asset_type = 'PyrCrm::Ecard'
LEFT OUTER JOIN pyr_monat_prd.pyr_events ev
  ON l.asset_id = ev.id
  AND l.asset_type = 'PyrCrm::Event'
LEFT OUTER JOIN pyr_monat_prd.pyr_sites pwps
  ON l.containable_type = 'PyrPwp:Site'
  AND l.containable_id = pwps.id
LEFT OUTER JOIN pyr_monat_prd.pyr_microsites pwpms
  ON l.containable_type = 'PyrPwp::Microsite'
  AND l.containable_id = pwpms.id
LEFT OUTER JOIN pyr_monat_prd.pyr_messages m
  ON l.channel_type = 'PyrCrm::Message'
  AND l.channel_id = m.id
LEFT OUTER JOIN pyr_monat_prd.pyr_sms sms
  ON l.channel_type = 'PyrCrm::Sms'
  AND l.channel_id = sms.id
INNER JOIN vibe_staging.users u
  ON l.user_id = u.user_id
  AND u.client_partition_id = 1
