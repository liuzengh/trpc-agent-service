package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct{ pool *pgxpool.Pool }

func NewPostgresRepository(ctx context.Context, dsn string) (*PostgresRepository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PostgresRepository{pool: pool}, nil
}

func (r *PostgresRepository) Migrate(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}
	return nil
}

func (r *PostgresRepository) SeedTenants(ctx context.Context, tenants []tenant.Tenant) error {
	for _, item := range tenants {
		payload, err := json.Marshal(item)
		if err != nil {
			return err
		}
		_, err = r.pool.Exec(ctx, `INSERT INTO tenants(id,name,enabled,profile,updated_at)
			VALUES($1,$2,$3,$4,now()) ON CONFLICT(id) DO UPDATE SET
			name=excluded.name,enabled=excluded.enabled,profile=excluded.profile,updated_at=now()`,
			item.ID, item.Name, item.Enabled, payload)
		if err != nil {
			return fmt.Errorf("seed tenant %s: %w", item.ID, err)
		}
	}
	return nil
}

func (r *PostgresRepository) ListTenants(ctx context.Context) ([]tenant.Tenant, error) {
	rows, err := r.pool.Query(ctx, `SELECT profile FROM tenants ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []tenant.Tenant
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var item tenant.Tenant
		if err := json.Unmarshal(payload, &item); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *PostgresRepository) CreateAgent(ctx context.Context, tenantID, id, name string) error {
	_, err := r.pool.Exec(ctx, `INSERT INTO agent_apps(tenant_id,id,name) VALUES($1,$2,$3)`, tenantID, id, name)
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}
	return nil
}

func (r *PostgresRepository) CreateAgentVersion(ctx context.Context, tenantID, agentID, version string, profile json.RawMessage) error {
	tag, err := r.pool.Exec(ctx, `INSERT INTO agent_versions(tenant_id,agent_id,version,profile)
		SELECT $1,$2,$3,$4 WHERE EXISTS(SELECT 1 FROM agent_apps WHERE tenant_id=$1 AND id=$2)
		ON CONFLICT DO NOTHING`, tenantID, agentID, version, profile)
	if err != nil {
		return fmt.Errorf("create agent version: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("agent missing or version already exists")
	}
	return nil
}

func (r *PostgresRepository) PublishAgent(ctx context.Context, tenantID, agentID, version string) error {
	tag, err := r.pool.Exec(ctx, `UPDATE agent_apps SET published_version=$3
		WHERE tenant_id=$1 AND id=$2 AND EXISTS(
			SELECT 1 FROM agent_versions WHERE tenant_id=$1 AND agent_id=$2 AND version=$3)`,
		tenantID, agentID, version)
	if err != nil {
		return fmt.Errorf("publish agent: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("agent or version not found")
	}
	_, err = r.pool.Exec(ctx, `UPDATE agent_versions SET status=CASE WHEN version=$3 THEN 'published' ELSE status END
		WHERE tenant_id=$1 AND agent_id=$2`, tenantID, agentID, version)
	return err
}

func (r *PostgresRepository) SaveChannelBinding(ctx context.Context, tenantID string, binding tenant.ChannelBinding) error {
	payload, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `INSERT INTO channel_bindings(tenant_id,id,channel,credential_ref,enabled,profile)
		VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id,id) DO UPDATE SET
		channel=excluded.channel,credential_ref=excluded.credential_ref,enabled=excluded.enabled,profile=excluded.profile`,
		tenantID, binding.ID, binding.Type, binding.CredentialRef, binding.Enabled, payload)
	return err
}

func (r *PostgresRepository) SaveBackendProfile(ctx context.Context, tenantID, id string, profile tenant.BackendProfile) error {
	payload, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `INSERT INTO backend_profiles(tenant_id,id,profile) VALUES($1,$2,$3)
		ON CONFLICT(tenant_id,id) DO UPDATE SET profile=excluded.profile`, tenantID, id, payload)
	return err
}

func (r *PostgresRepository) AcceptInbound(ctx context.Context, msg channels.InboundEnvelope) (bool, error) {
	if err := msg.Validate(); err != nil {
		return false, err
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return false, err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	key := msg.IdempotencyKey()
	tag, err := tx.Exec(ctx, `INSERT INTO inbound_messages
		(id,tenant_id,binding_id,channel,external_message_id,payload,received_at)
		VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, key, msg.TenantID,
		msg.BindingID, msg.Channel, msg.ExternalMessageID, payload, msg.ReceivedAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO dispatch_outbox(id,payload,status,next_attempt_at)
		VALUES($1,$2,'pending',now())`, key, payload); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (r *PostgresRepository) ClaimDispatch(ctx context.Context, worker string, limit int, lease time.Duration) ([]DispatchTask, error) {
	rows, err := r.pool.Query(ctx, claimDispatchSQL, worker, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []DispatchTask
	for rows.Next() {
		var id string
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, err
		}
		var msg channels.InboundEnvelope
		if err := json.Unmarshal(payload, &msg); err != nil {
			return nil, err
		}
		result = append(result, DispatchTask{ID: id, Message: msg})
	}
	return result, rows.Err()
}

func (r *PostgresRepository) CompleteDispatch(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE dispatch_outbox SET status='done',updated_at=now() WHERE id=$1`, id)
	return err
}

func (r *PostgresRepository) RetryDispatch(ctx context.Context, id string, cause error) error {
	_, err := r.pool.Exec(ctx, retrySQL("dispatch_outbox"), id, errorText(cause))
	return err
}

func (r *PostgresRepository) StoreReply(ctx context.Context, msg channels.OutboundEnvelope) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `INSERT INTO reply_outbox
		(id,tenant_id,binding_id,channel,payload,status,next_attempt_at)
		VALUES($1,$2,$3,$4,$5,'pending',now()) ON CONFLICT(id) DO NOTHING`,
		msg.ID, msg.TenantID, msg.BindingID, msg.Channel, payload)
	return err
}

func (r *PostgresRepository) CommitAgentResult(ctx context.Context, in channels.InboundEnvelope, out channels.OutboundEnvelope) error {
	if out.ID == "" {
		return errors.New("reply id is required")
	}
	inPayload, err := json.Marshal(map[string]any{
		"role": "user", "content": in.Content, "trace_id": in.TraceID,
		"external_message_id": in.ExternalMessageID,
	})
	if err != nil {
		return err
	}
	outPayload, err := json.Marshal(map[string]any{
		"role": "assistant", "content": out.Content, "trace_id": out.TraceID,
	})
	if err != nil {
		return err
	}
	replyPayload, err := json.Marshal(out)
	if err != nil {
		return err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var version int64
	err = tx.QueryRow(ctx, `INSERT INTO sessions(tenant_id,session_id,version,state,updated_at)
		VALUES($1,$2,1,'{}',now()) ON CONFLICT(tenant_id,session_id) DO UPDATE SET
		version=sessions.version+1,updated_at=now() RETURNING version`,
		in.TenantID, in.SessionID()).Scan(&version)
	if err != nil {
		return fmt.Errorf("advance session version: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_events(tenant_id,session_id,sequence_no,event)
		VALUES($1,$2,$3,$4),($1,$2,$5,$6) ON CONFLICT DO NOTHING`,
		in.TenantID, in.SessionID(), version*2-1, inPayload, version*2, outPayload); err != nil {
		return fmt.Errorf("append session events: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO reply_outbox
		(id,tenant_id,binding_id,channel,payload,status,next_attempt_at)
		VALUES($1,$2,$3,$4,$5,'pending',now()) ON CONFLICT(id) DO NOTHING`,
		out.ID, out.TenantID, out.BindingID, out.Channel, replyPayload); err != nil {
		return fmt.Errorf("store reply outbox: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *PostgresRepository) ReplyExists(ctx context.Context, id string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM reply_outbox WHERE id=$1)`, id).Scan(&exists)
	return exists, err
}

func (r *PostgresRepository) ClaimReplies(ctx context.Context, worker string, limit int, lease time.Duration) ([]ReplyTask, error) {
	rows, err := r.pool.Query(ctx, claimReplySQL, worker, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ReplyTask
	for rows.Next() {
		var id string
		var payload []byte
		var attempts int
		if err := rows.Scan(&id, &payload, &attempts); err != nil {
			return nil, err
		}
		var msg channels.OutboundEnvelope
		if err := json.Unmarshal(payload, &msg); err != nil {
			return nil, err
		}
		msg.Attempts = attempts
		result = append(result, ReplyTask{ID: id, Message: msg})
	}
	return result, rows.Err()
}

func (r *PostgresRepository) CompleteReply(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE reply_outbox SET status='done',updated_at=now() WHERE id=$1`, id)
	return err
}

func (r *PostgresRepository) RetryReply(ctx context.Context, id string, cause error) error {
	_, err := r.pool.Exec(ctx, retrySQL("reply_outbox"), id, errorText(cause))
	return err
}

func (r *PostgresRepository) AppendAudit(ctx context.Context, a AuditLog) error {
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	_, err := r.pool.Exec(ctx, `INSERT INTO audit_logs
		(tenant_id,channel,user_id,session_id,agent_name,tool_name,decision,latency_ms,error_type,cost,trace_id,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, a.TenantID, a.Channel,
		a.UserID, a.SessionID, a.AgentName, a.ToolName, a.Decision, a.Latency.Milliseconds(),
		a.ErrorType, a.Cost, a.TraceID, a.CreatedAt)
	return err
}

func (r *PostgresRepository) Stats(ctx context.Context) (Stats, error) {
	var result Stats
	err := r.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM inbound_messages),
		(SELECT count(*) FROM dispatch_outbox WHERE status<>'done'),
		(SELECT count(*) FROM reply_outbox WHERE status<>'done'),
		(SELECT count(*) FROM audit_logs)`).Scan(&result.InboundTotal, &result.DispatchReady, &result.ReplyReady, &result.AuditTotal)
	return result, err
}

func (r *PostgresRepository) Close() { r.pool.Close() }

func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > 512 {
		text = text[:512]
	}
	return text
}

func retrySQL(table string) string {
	if table != "dispatch_outbox" && table != "reply_outbox" {
		panic(errors.New("invalid outbox table"))
	}
	return fmt.Sprintf(`UPDATE %s SET status=CASE WHEN attempts>=8 THEN 'dead' ELSE 'pending' END,
		last_error=$2,next_attempt_at=now()+(LEAST(attempts,6)*interval '2 seconds'),
		locked_until=NULL,locked_by=NULL,updated_at=now() WHERE id=$1`, table)
}

const claimDispatchSQL = `WITH picked AS (
	SELECT id FROM dispatch_outbox
	WHERE status IN ('pending','processing') AND next_attempt_at<=now()
	AND (locked_until IS NULL OR locked_until<now())
	ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $2
) UPDATE dispatch_outbox o SET status='processing',locked_by=$1,
	locked_until=now()+($3*interval '1 millisecond'),attempts=o.attempts+1,updated_at=now()
	FROM picked WHERE o.id=picked.id RETURNING o.id,o.payload`

const claimReplySQL = `WITH picked AS (
	SELECT id FROM reply_outbox
	WHERE status IN ('pending','processing') AND next_attempt_at<=now()
	AND (locked_until IS NULL OR locked_until<now())
	ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $2
) UPDATE reply_outbox o SET status='processing',locked_by=$1,
	locked_until=now()+($3*interval '1 millisecond'),attempts=o.attempts+1,updated_at=now()
	FROM picked WHERE o.id=picked.id RETURNING o.id,o.payload,o.attempts`

const schemaSQL = `
CREATE TABLE IF NOT EXISTS tenants(
  id text PRIMARY KEY,name text NOT NULL,enabled boolean NOT NULL,profile jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS agent_apps(
  tenant_id text NOT NULL REFERENCES tenants(id),id text NOT NULL,name text NOT NULL,
  published_version text,PRIMARY KEY(tenant_id,id));
CREATE TABLE IF NOT EXISTS agent_versions(
  tenant_id text NOT NULL,agent_id text NOT NULL,version text NOT NULL,profile jsonb NOT NULL,
  status text NOT NULL DEFAULT 'draft',created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(tenant_id,agent_id,version));
CREATE TABLE IF NOT EXISTS channel_bindings(
  tenant_id text NOT NULL REFERENCES tenants(id),id text NOT NULL,channel text NOT NULL,
  credential_ref text NOT NULL,enabled boolean NOT NULL DEFAULT false,profile jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY(tenant_id,id));
CREATE TABLE IF NOT EXISTS backend_profiles(
  tenant_id text NOT NULL REFERENCES tenants(id),id text NOT NULL,profile jsonb NOT NULL,
  PRIMARY KEY(tenant_id,id));
CREATE TABLE IF NOT EXISTS sessions(
  tenant_id text NOT NULL,session_id text NOT NULL,version bigint NOT NULL DEFAULT 0,
  state jsonb NOT NULL DEFAULT '{}',updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(tenant_id,session_id));
CREATE TABLE IF NOT EXISTS session_events(
  tenant_id text NOT NULL,session_id text NOT NULL,sequence_no bigint NOT NULL,event jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(tenant_id,session_id,sequence_no));
CREATE TABLE IF NOT EXISTS memories(
  tenant_id text NOT NULL,id text NOT NULL,session_id text,value jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(tenant_id,id));
CREATE TABLE IF NOT EXISTS summaries(
  tenant_id text NOT NULL,session_id text NOT NULL,version bigint NOT NULL,summary text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(tenant_id,session_id,version));
CREATE TABLE IF NOT EXISTS inbound_messages(
  id text PRIMARY KEY,tenant_id text NOT NULL,binding_id text NOT NULL,channel text NOT NULL,
  external_message_id text NOT NULL,payload jsonb NOT NULL,received_at timestamptz NOT NULL,
  UNIQUE(tenant_id,binding_id,channel,external_message_id));
CREATE TABLE IF NOT EXISTS dispatch_outbox(
  id text PRIMARY KEY,payload jsonb NOT NULL,status text NOT NULL,attempts int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL,locked_by text,locked_until timestamptz,last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS reply_outbox(
  id text PRIMARY KEY,tenant_id text NOT NULL,binding_id text NOT NULL,channel text NOT NULL,
  payload jsonb NOT NULL,status text NOT NULL,attempts int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL,locked_by text,locked_until timestamptz,last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS audit_logs(
  id bigserial PRIMARY KEY,tenant_id text NOT NULL,channel text NOT NULL,user_id text NOT NULL,
  session_id text NOT NULL,agent_name text NOT NULL,tool_name text,decision text NOT NULL,
  latency_ms bigint NOT NULL,error_type text,cost double precision NOT NULL DEFAULT 0,
  trace_id text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS dispatch_outbox_ready ON dispatch_outbox(status,next_attempt_at);
CREATE INDEX IF NOT EXISTS reply_outbox_ready ON reply_outbox(status,next_attempt_at);
CREATE INDEX IF NOT EXISTS audit_tenant_time ON audit_logs(tenant_id,created_at DESC);`
