-- One Binding keeps its exact stable target and may publish one exact canary
-- target. The complete non-secret policy remains in the Binding aggregate and
-- route event; the canary revision itself is immutable and resolved by Control
-- before this JSON document is stored.
ALTER TABLE channel_bindings
    ADD COLUMN traffic_policy_jsonb jsonb
    CHECK (traffic_policy_jsonb IS NULL OR jsonb_typeof(traffic_policy_jsonb)='object');

ALTER TABLE channel_command_receipts
    DROP CONSTRAINT channel_command_receipts_operation_check;
ALTER TABLE channel_command_receipts
    ADD CHECK (operation IN (
        'CreateChannelAccount',
        'UpdateChannelAccount',
        'UpdateAccountCredential',
        'SetChannelAccountEnabled',
        'CreateChannelBinding',
        'SetChannelBindingTarget',
        'SetChannelBindingTraffic',
        'SetChannelBindingEnabled'
    ));
