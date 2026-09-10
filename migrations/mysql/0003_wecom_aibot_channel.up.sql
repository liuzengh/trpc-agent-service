ALTER TABLE channel_binding
    DROP CHECK channel_binding_channel_ck;

ALTER TABLE channel_binding
    ADD CONSTRAINT channel_binding_channel_ck
    CHECK (channel IN ('wecom', 'wecom_aibot', 'telegram'));
