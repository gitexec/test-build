##-#
DROP TABLE IF EXISTS `data-science`.`emails`;

CREATE TABLE IF NOT EXISTS `data-science`.`emails` (
  sender_tree_user_id int unsigned null,
  recipient_tree_user_id int unsigned null,
  `message_id` int(11) NOT NULL,
  `sender_id` int(11) DEFAULT NULL,
  `sender_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `sent_date` datetime DEFAULT NULL,
  `message_to` text COLLATE utf8_unicode_ci DEFAULT NULL,
  `from` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `subject` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `message` text COLLATE utf8_unicode_ci,
  `short_message` varchar(140) COLLATE utf8_unicode_ci DEFAULT NULL,
  `draft` tinyint(1) DEFAULT '1',
  `delivery_status` varchar(255) COLLATE utf8_unicode_ci DEFAULT 'unsent',
  `reply_to_id` int(11) DEFAULT NULL,
  `forwarded_from_id` int(11) DEFAULT NULL,
  `internal_recipient_count` int(11) DEFAULT NULL,
  `internal_open_count` int(11) DEFAULT NULL,
  `size` int(11) DEFAULT NULL,
  `has_attachments` tinyint(1) DEFAULT '0',
  `message_extras` text COLLATE utf8_unicode_ci,
  `message_created_at` datetime DEFAULT NULL,
  `message_updated_at` datetime DEFAULT NULL,
  `message_type` int(11) DEFAULT '0',
  `system_generated` tinyint(1) DEFAULT '0',
  `message_tag_deleted` tinyint(1) DEFAULT '0',
  `message_tag_trash` tinyint(1) DEFAULT '0',
  `message_tag_important` tinyint(1) DEFAULT '0',
  `message_tag_starred` tinyint(1) DEFAULT '0',
  `message_cc` text COLLATE utf8_unicode_ci DEFAULT NULL,
  `message_bcc` text COLLATE utf8_unicode_ci DEFAULT NULL,
  `undeliverable_count` int(11) DEFAULT NULL,
  `spam_reported_count` int(11) DEFAULT NULL,
  `tag_spam` tinyint(1) DEFAULT '0',
  `recipient_id` int(11) DEFAULT NULL,
  `recipient_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `consultant_id` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `delivery_type` varchar(255) COLLATE utf8_unicode_ci DEFAULT 'email',
  `recipient_to` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `subject_interpolated` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `message_interpolated` text COLLATE utf8_unicode_ci DEFAULT NULL,
  `opened_count` int(11) DEFAULT '0',
  `recipient_tag_starred` tinyint(1) DEFAULT '0',
  `recipient_tag_important` tinyint(1) DEFAULT '0',
  `recipient_tag_spam` tinyint(1) DEFAULT '0',
  `recipient_tag_trash` tinyint(1) DEFAULT '0',
  `recipient_tag_deleted` tinyint(1) DEFAULT '0',
  `from_pyr_user` tinyint(1) DEFAULT '0',
  `from_community` tinyint(1) DEFAULT '0',
  `from_downline` tinyint(1) DEFAULT '0',
  `from_upline` tinyint(1) DEFAULT '0',
  `from_team` tinyint(1) DEFAULT '0',
  `replied_to_message_id` int(11) DEFAULT NULL,
  `forwarded_message_id` int(11) DEFAULT NULL,
  `message_header_id` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `recipient_extras` text COLLATE utf8_unicode_ci,
  `recipient_created_at` datetime DEFAULT NULL,
  `recipient_updated_at` datetime DEFAULT NULL,
  `read` datetime DEFAULT NULL,
  `from_contact` tinyint(1) DEFAULT '0',
  `seen` datetime DEFAULT NULL,
  `undeliverable` tinyint(1) DEFAULT '0',
  `recipient_cc` tinyint(1) DEFAULT NULL,
  `recipient_bcc` tinyint(1) DEFAULT NULL,
  `spam_reported` tinyint(1) DEFAULT '0'
);
#-##

REPLACE INTO `data-science`.`emails`
SELECT
  CASE
    WHEN mc.id IS NOT NULL THEN mc.tree_user_id
    WHEN mu.id IS NOT NULL THEN mu.tree_user_id
  END AS `message_tree_user_id`,
  CASE
    WHEN tu.id IS NOT NULL THEN tu.id
    WHEN c.id IS NOT NULL THEN c.tree_user_id
    WHEN u.id IS NOT NULL THEN u.tree_user_id
  END AS `recipient_tree_user_id`,
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
FROM pyr_messages m
INNER JOIN pyr_message_recipients mr
  ON m.id = mr.message_id
  AND mr.delivery_type = 'email'
LEFT OUTER JOIN tree_users tu
  ON mr.recipient_type = 'PyrTree::User'
  AND mr.recipient_id = tu.id
LEFT OUTER JOIN pyr_contacts c
  ON mr.recipient_type = 'PyrCrm::Contact'
  AND mr.recipient_id = c.id
LEFT OUTER JOIN users u
  ON mr.recipient_type = 'User'
  AND mr.recipient_id = u.id
LEFT OUTER JOIN users mu
  ON m.sender_type = 'User'
  AND m.sender_id = mu.id
LEFT OUTER JOIN pyr_contacts mc
  ON m.sender_type = 'PyrCrm::Contact'
  AND m.sender_id = mc.id;
