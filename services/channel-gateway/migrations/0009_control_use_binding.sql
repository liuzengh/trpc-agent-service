-- Claims capture the exact local account qualification and SDK generation.
ALTER TABLE gateway_delivery_parts ADD COLUMN claim_use_binding text NOT NULL DEFAULT '';
ALTER TABLE gateway_delivery_attempts ADD COLUMN account_use_binding text NOT NULL DEFAULT '';
