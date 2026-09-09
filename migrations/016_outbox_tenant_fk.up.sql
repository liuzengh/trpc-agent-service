-- Enforce the tenant invariant at the database seam as well as in
-- CompleteInbox. Direct SQL or a future adapter must not attach an Outbox row
-- to an Inbox belonging to another tenant.
CREATE UNIQUE INDEX uq_inbox_id_tenant
    ON inbox_messages (id, tenant_id);

ALTER TABLE outbox_messages
    ADD CONSTRAINT outbox_inbox_tenant_fk
    FOREIGN KEY (inbox_id, tenant_id)
    REFERENCES inbox_messages (id, tenant_id);
