-- Keep operator-visible migration errors bounded. Existing rows that violate
-- this invariant intentionally fail the migration so an operator can inspect
-- and clean them rather than silently truncating potentially sensitive data.
ALTER TABLE data_migrations
    ADD CONSTRAINT data_migrations_last_error_size
    CHECK (octet_length(last_error) <= 4096);
