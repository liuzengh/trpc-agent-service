ALTER TABLE agent_app
    ADD COLUMN canary_revision BIGINT NULL AFTER current_revision;

ALTER TABLE agent_app
    ADD KEY agent_app_canary_revision_idx (tenant_id, app_id, canary_revision);

ALTER TABLE agent_app
    ADD CONSTRAINT agent_app_canary_revision_fk
    FOREIGN KEY (tenant_id, app_id, canary_revision)
    REFERENCES agent_app_revision (tenant_id, app_id, revision);

CREATE TRIGGER agent_app_canary_guard_upd
BEFORE UPDATE ON agent_app
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    IF NEW.canary_revision IS NOT NULL THEN
        IF NEW.current_revision IS NULL OR NEW.canary_revision = NEW.current_revision THEN
            SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app canary revision must differ from current revision';
        END IF;
        SELECT state INTO revision_state
        FROM agent_app_revision
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.canary_revision;
        IF revision_state IS NULL OR revision_state <> 'published' THEN
            SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app canary revision must be published';
        END IF;
    END IF;
END;
