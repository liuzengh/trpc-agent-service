DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT con.conname
        FROM pg_constraint con
        WHERE con.conrelid = 'platform.execution'::regclass
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) LIKE '%PENDING%'
          AND pg_get_constraintdef(con.oid) LIKE '%RUNNING%'
          AND pg_get_constraintdef(con.oid) LIKE '%CANCELED%'
    LOOP
        EXECUTE format('ALTER TABLE platform.execution DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE platform.execution
    ADD CONSTRAINT execution_status_check CHECK (
        status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'UNCERTAIN', 'CANCELED')
    );
