-- A sender may have completed the external provider call before losing the
-- local lease. Unknown results are terminal for automatic delivery and require
-- operator/provider reconciliation.

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT con.conname
        FROM pg_constraint con
        WHERE con.conrelid = 'platform.reply_outbox'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%PENDING%'
          AND pg_get_constraintdef(con.oid) LIKE '%SENDING%'
          AND pg_get_constraintdef(con.oid) LIKE '%PERMANENTLY_FAILED%'
    LOOP
        EXECUTE format('ALTER TABLE platform.reply_outbox DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE platform.reply_outbox
    ADD CONSTRAINT reply_outbox_status_check CHECK (
        status IN ('PENDING', 'SENDING', 'SENT', 'UNCERTAIN', 'PERMANENTLY_FAILED')
    );
