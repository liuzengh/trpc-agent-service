ALTER TABLE public.channel_binding
    DROP CONSTRAINT channel_binding_channel_check;

ALTER TABLE public.channel_binding
    ADD CONSTRAINT channel_binding_channel_check
    CHECK (channel IN ('wecom', 'wecom_aibot', 'telegram'));
