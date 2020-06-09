## wv
UPDATE vibe_staging.users
    SET type = CASE
        WHEN client_type LIKE '%Customer%' OR client_type IN ('Employee', 'RCSCompany') THEN 'Autoship'
        WHEN client_type IN ('B2B', 'Import', 'Distributor') OR client_type LIKE '%Transfer%' THEN 'Distributor'
    END
WHERE client_partition_id = 2
AND type = 'tbd';

UPDATE vibe_staging.users
    SET status = CASE
        WHEN client_status IN ('Active', 'Grace', 'Hold') THEN 'Active'
        ELSE 'Inactive'
    END
WHERE client_partition_id = 2
AND status = 'tbd';

## nsp
UPDATE vibe_staging.users
    SET type = CASE
        WHEN client_type = 'MEMBER' THEN 'Distributor'
        WHEN client_type = 'RETAIL' THEN 'Customer'
        WHEN client_type = 'BA' THEN 'Distributor'
        WHEN client_type = 'PC' THEN 'Autoship'
    END
WHERE type = 'tbd'
AND icentris_client = 'naturessunshine'

UPDATE vibe_staging.users
    SET status = client_status
WHERE status = 'tbd'
AND icentris_client = 'naturessunshine'


