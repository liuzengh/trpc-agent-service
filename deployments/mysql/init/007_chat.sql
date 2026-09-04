-- =============================================================================
-- 007_chat.sql — Chat ledger (sessions + messages + summary + memory)
--
-- NOTE on runtime vs ledger: the ACTUAL session state / event stream / summary
-- live in the tRPC-Agent-Go session backend (session/mysql auto-creates
-- session_states / session_events / session_summaries / app_states / user_states,
-- optionally prefixed). The tables here are the BUSINESS ledger, decoupled from
-- the prompt-context sliding window, used for session listing, turn pagination,
-- cost attribution, and SSE resourceId exposure.
-- =============================================================================

CREATE TABLE IF NOT EXISTS chat_sessions (
    session_id       VARCHAR(128) NOT NULL          COMMENT 'business UUID; also the framework session key SessionID',
    tenant_id        VARCHAR(36)  NOT NULL          COMMENT 'framework session AppName',
    agent_id         VARCHAR(36)  NOT NULL,
    member_id        VARCHAR(64)  NOT NULL          COMMENT 'initiating member (framework UserID)',
    channel          VARCHAR(32)  NOT NULL          COMMENT 'wecom | feishu | admin',
    status           VARCHAR(32)  NOT NULL DEFAULT 'ACTIVE',
    memory_cutoff_at DATETIME     NULL              COMMENT 'soft-reset watermark; history before it is filtered (not hard-deleted)',
    last_message_at  DATETIME     NULL              COMMENT 'for session list ordering + activity stats',
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id),
    KEY idx_chat_session_agent_member (agent_id, member_id),
    KEY idx_chat_session_tenant (tenant_id),
    KEY idx_chat_session_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='chat session ledger; one session = one member + one agent';

CREATE TABLE IF NOT EXISTS chat_messages (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    message_id     VARCHAR(64)  NOT NULL             COMMENT 'business UUID; exposed as SSE resourceId (anti-enumeration)',
    session_id     VARCHAR(128) NOT NULL,
    agent_id       VARCHAR(36)  NOT NULL             COMMENT 'redundant for agent-dimension queries',
    member_id      VARCHAR(64)  NOT NULL             COMMENT 'redundant for member-dimension queries',
    turn_id        VARCHAR(64)  NOT NULL             COMMENT 'one USER + ASSISTANT stream + its TOOL_CALL/TOOL_RESULT share a turn',
    turn_timestamp BIGINT       NOT NULL             COMMENT 'ms epoch; turn-desc pagination (SELECT DISTINCT turn then IN)',
    role           VARCHAR(32)  NOT NULL             COMMENT 'USER | ASSISTANT | TOOL_CALL | TOOL_RESULT',
    content        MEDIUMTEXT   NOT NULL             COMMENT 'tool messages hold JSON {tool,input,status,output}',
    status         VARCHAR(32)  NOT NULL DEFAULT 'SUCCESS'
                                                      COMMENT 'INIT | PROCESSING | SUCCESS | FAILED | DELETED',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_chat_message_message_id (message_id),
    KEY idx_chat_message_session_turn (session_id, turn_timestamp),
    KEY idx_chat_message_agent (agent_id),
    KEY idx_chat_message_member (member_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='fact ledger: full record of dialogue + tool calls; unaffected by prompt window truncation';

-- chat_session_summaries / memories tables were removed in stage 33: session
-- summaries and long-term memory are owned by the tRPC-Agent-Go backends
-- (framework session/mysql keeps its own session_summaries; memory lives in
-- memory/redis or memory/inmemory). These platform tables duplicated that
-- responsibility and were never read or written.
