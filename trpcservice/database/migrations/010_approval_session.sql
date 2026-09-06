CREATE INDEX idx_tool_approval_pending_session
    ON tool_approval(tenant_id, channel_binding_id, user_id, session_id, created_at)
    WHERE status = 'pending';
