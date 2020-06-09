INSERT INTO vibe_staging.emails (
    icentris_client,
    client_partition_id,
    leo_id,
    ingestion_timestamp,
    sender_tree_user_id,
    message_id,
    sender_id,
    sender_type,
    sent_date,
    message_to,
    `from`,
    subject,
    message,
    short_message,
    draft,
    delivery_status,
    reply_to_id,
    forwarded_from_id,
    internal_recipient_count,
    internal_open_count,
    size,
    has_attachments,
    message_extras,
    message_created_at,
    message_updated_at,
    message_type,
    system_generated,
    message_tag_deleted,
    message_tag_trash,
    message_tag_important,
    message_tag_starred,
    message_cc,
    message_bcc,
    undeliverable_count,
    spam_reported_count,
    tag_spam,
    recipients
)
SELECT
  'monat' AS icentris_client,
  CASE
    WHEN mc.id IS NOT NULL THEN mc.tree_user_id
    WHEN mu.id IS NOT NULL THEN mu.tree_user_id
  END AS `message_tree_user_id`,
  m.id AS `message_id`,
  m.sender_id,
  m.sender_type,
  m.sent_date,
  m.to,
  m.from,
  m.subject,
  m.message,
  m.short_message,
  m.draft,
  m.delivery_status,
  m.reply_to_id,
  m.forwarded_from_id,
  m.internal_recipient_count,
  m.internal_open_count,
  m.size,
  m.has_attachments,
  m.extras AS `message_extras`,
  m.created_at AS `message_created_at`,
  m.updated_at AS `message_updated_at`,
  m.message_type,
  m.system_generated,
  m.tag_deleted AS `message_tag_deleted`,
  m.tag_trash AS `message_tag_trash`,
  m.tag_important AS `message_tag_important`,
  m.tag_starred AS `message_tag_starred`,
  m.cc AS `message_cc`,
  m.bcc AS `message_bcc`,
  m.undeliverable_count,
  m.spam_reported_count,
  m.tag_spam,
  ARRAY_AGG( STRUCT(
  CASE
    WHEN tu.id IS NOT NULL THEN tu.id
    WHEN c.id IS NOT NULL THEN c.tree_user_id
    WHEN u.id IS NOT NULL THEN u.tree_user_id
  END AS `tree_user_id`,
  mr.recipient_id,
    mr.recipient_type,
    mr.consultant_id,
    mr.delivery_type,
    mr.to AS `recipient_to`,
    mr.subject_interpolated,
    mr.message_interpolated,
    mr.opened_count,
    mr.tag_starred AS `recipient_tag_starred`,
    mr.tag_important AS `recipient_tag_important`,
    mr.tag_spam AS `recipient_tag_spam`,
    mr.tag_trash AS `recipient_tag_trash`,
    mr.tag_deleted AS `recipient_tag_deleted`,
    mr.from_pyr_user,
    mr.from_community,
    mr.from_downline,
    mr.from_upline,
    mr.from_team,
    mr.replied_to_message_id,
    mr.forwarded_message_id,
    mr.message_header_id,
    mr.extras AS `recipient_extras`,
    mr.created_at AS `recipient_created_at`,
    mr.updated_at AS `recipient_updated_at`,
    mr.read,
    mr.from_contact,
    mr.seen,
    mr.undeliverable,
    mr.cc AS `recipient_cc`,
    mr.bcc AS `recipient_bcc`,
    mr.spam_reported
))
FROM pyr_monat_prd.pyr_messages m
LEFT OUTER JOIN pyr_monat_prd.pyr_message_recipients mr
  ON m.id = mr.message_id
  AND mr.delivery_type = 'email'
LEFT OUTER JOIN pyr_monat_prd.tree_users tu
  ON mr.recipient_type = 'PyrTree::User'
  AND mr.recipient_id = tu.id
LEFT OUTER JOIN pyr_monat_prd.pyr_contacts c
  ON mr.recipient_type = 'PyrCrm::Contact'
  AND mr.recipient_id = c.id
LEFT OUTER JOIN pyr_monat_prd.users u
  ON mr.recipient_type = 'User'
  AND mr.recipient_id = u.id
LEFT OUTER JOIN pyr_monat_prd.users mu
  ON m.sender_type = 'User'
  AND m.sender_id = mu.id
LEFT OUTER JOIN pyr_monat_prd.pyr_contacts mc
  ON m.sender_type = 'PyrCrm::Contact'
  AND m.sender_id = mc.id
GROUP BY icentris_client,
  `message_tree_user_id`,
  `recipient_tree_user_id`,
  `message_id`,
  m.sender_id,
  m.sender_type,
  m.sent_date,
  m.to,
  m.from,
  m.subject,
  m.message,
  m.short_message,
  m.draft,
  m.delivery_status,
  m.reply_to_id,
  m.forwarded_from_id,
  m.internal_recipient_count,
  m.internal_open_count,
  m.size,
  m.has_attachments,
  `message_extras`,
  `message_created_at`,
  `message_updated_at`,
  m.message_type,
  m.system_generated,
  `message_tag_deleted`,
  `message_tag_trash`,
  `message_tag_important`,
  `message_tag_starred`,
  `message_cc`,
  `message_bcc`,
  m.undeliverable_count,
  m.spam_reported_count,
  m.tag_spam

