-- Record every accepted decision message, including user-initiated repeats.
-- This prevents a repeated message ID from deciding a different approval.
CREATE TABLE approval_decision_message (
    channel_binding_id VARCHAR(64) NOT NULL,
    external_message_id VARCHAR(512) NOT NULL,
    approval_id VARCHAR(64) NOT NULL REFERENCES tool_approval(approval_id),
    status VARCHAR(32) NOT NULL CHECK(status IN ('approved','denied')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(channel_binding_id, external_message_id)
);
INSERT INTO approval_decision_message(channel_binding_id,external_message_id,approval_id,status)
SELECT channel_binding_id,decision_message_id,approval_id,status FROM tool_approval
WHERE decision_message_id IS NOT NULL AND status IN ('approved','denied');
