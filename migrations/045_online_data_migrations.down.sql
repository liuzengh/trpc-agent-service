-- Rollback of the schema requires every migration to be terminal. Removing
-- active routes would let an old cached worker resume writing an old profile.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM data_migration_live_routes WHERE mirroring) THEN
        RAISE EXCEPTION 'online data migrations must be completed or rolled back first';
    END IF;
END $$;
DROP TABLE data_migration_live_journal;
DROP TABLE data_migration_live_intents;
DROP TABLE data_migration_live_routes;
