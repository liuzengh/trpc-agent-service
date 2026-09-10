-- 013_member_single_tenant.sql — enforce one tenant per login member.
--
-- Existing installations must be checked for duplicate user_id values before
-- applying this migration. The application has always treated user_id as the
-- login identity, so duplicates are ambiguous and cannot be migrated safely.
SET @password_hash_exists := (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'tenant_members'
      AND COLUMN_NAME = 'password_hash'
);
SET @sql := IF(
    @password_hash_exists = 0,
    'ALTER TABLE tenant_members ADD COLUMN password_hash VARCHAR(255) NOT NULL DEFAULT '''' AFTER role',
    'SELECT 1'
);
PREPARE add_password_hash FROM @sql;
EXECUTE add_password_hash;
DEALLOCATE PREPARE add_password_hash;

SET @member_user_unique_exists := (
    SELECT COUNT(*)
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'tenant_members'
      AND INDEX_NAME = 'uk_tenant_members_user'
);
SET @sql := IF(
    @member_user_unique_exists = 0,
    'ALTER TABLE tenant_members ADD UNIQUE KEY uk_tenant_members_user (user_id)',
    'SELECT 1'
);
PREPARE add_member_user_unique FROM @sql;
EXECUTE add_member_user_unique;
DEALLOCATE PREPARE add_member_user_unique;
