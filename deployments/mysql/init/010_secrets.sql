-- 010_secrets.sql
-- Unified credential store. Values are encrypted at rest (AES-256-GCM) by the
-- application layer; the master key is never persisted here. Plaintext
-- credentials must never be written to domain tables (endpoints, channels).

CREATE TABLE IF NOT EXISTS secrets (
    secret_key  VARCHAR(255) NOT NULL,
    ciphertext  MEDIUMBLOB   NOT NULL COMMENT 'base64(nonce||AES-256-GCM ciphertext)',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (secret_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='encrypted credentials (AES-256-GCM); plaintext never stored';
