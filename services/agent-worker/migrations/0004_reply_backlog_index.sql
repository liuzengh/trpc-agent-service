-- Observe only outstanding replies without scanning retained published history.
CREATE INDEX execution_reply_outbox_pending ON execution_reply_outbox(created_at)
 WHERE published_at IS NULL;
