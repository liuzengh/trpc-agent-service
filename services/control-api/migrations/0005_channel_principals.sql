-- F01 identity persistence foundation. This migration does not grant channel
-- access, create tenant membership, enable accounts, or publish a policy.
-- Identity uniqueness is tenant/provider/account/external-user scoped. Revoke
-- retains the original mapping so a repeated registration cannot bypass it.
CREATE TABLE channel_principal_bindings (
    tenant_id text NOT NULL,
    principal_id text NOT NULL
        CHECK (principal_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    account_id text NOT NULL,
    provider text NOT NULL CHECK (provider IN ('telegram', 'wecom')),
    external_user_id text NOT NULL
        CHECK (octet_length(external_user_id) BETWEEN 1 AND 1024
               AND btrim(external_user_id) <> ''),
    state text NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'REVOKED')),
    revision bigint NOT NULL DEFAULT 1
        CHECK (revision BETWEEN 1 AND 9007199254740991),
    created_by text NOT NULL REFERENCES user_accounts(id),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, principal_id),
    UNIQUE (tenant_id, provider, account_id, external_user_id),
    -- Future policy-member references must include the account and provider,
    -- not only tenant/principal, to prevent implicit cross-Bot identity reuse.
    UNIQUE (tenant_id, principal_id, account_id, provider),
    FOREIGN KEY (tenant_id, account_id, provider)
        REFERENCES channel_accounts(tenant_id, id, provider),
    CHECK (updated_at >= created_at)
);
CREATE INDEX channel_principal_bindings_account_page
    ON channel_principal_bindings(tenant_id, account_id, principal_id);

-- Preserve the closed operation set while allowing the two new owner commands.
-- Their success receipt commits with the identity mutation, not afterward.
ALTER TABLE channel_command_receipts DROP CONSTRAINT channel_command_receipts_operation_check;
ALTER TABLE channel_command_receipts ADD CONSTRAINT channel_command_receipts_operation_check
    CHECK (operation IN ('CreateChannelAccount','UpdateChannelAccount',
      'UpdateAccountCredential','SetChannelAccountEnabled','CreateChannelBinding',
      'SetChannelBindingTarget','SetChannelBindingEnabled','RegisterChannelPrincipal',
      'SetChannelPrincipalState'));
