-- Allow the first composite Agent definition while keeping schema version 1.
ALTER TABLE agent_app_revision
    DROP CHECK agent_revision_kind_ck;

ALTER TABLE agent_app_revision
    ADD CONSTRAINT agent_revision_kind_ck
    CHECK (agent_kind IN ('llm', 'chain'));
