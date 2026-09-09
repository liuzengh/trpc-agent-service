package operations

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type viewSpec struct {
	from, tenant, id, timestamp, status, fields string
	numeric                                     bool
}

// SQL identifiers and projection fields are compile-time allowlists. In
// particular, no row_to_json, SELECT *, payload, config or raw error is used.
var views = map[string]viewSpec{
	"apps":        {"agent_apps a", "a.tenant_id", "a.id", "a.created_at", "a.status", "'name',a.name,'updatedAt',a.updated_at", false},
	"versions":    {"agent_versions v JOIN agent_apps a ON a.id=v.agent_app_id", "a.tenant_id", "v.id", "v.created_at", "v.status", "'appId',v.agent_app_id,'versionNumber',v.version_number,'publishedAt',v.published_at", false},
	"deployments": {"deployments d", "d.tenant_id", "d.id", "d.created_at", "d.status", "'appId',d.agent_app_id,'versionId',d.agent_version_id,'kind',d.kind,'trafficBps',d.traffic_bps,'updatedAt',d.updated_at", false},
	"inbox":       {"inbox_messages i", "i.tenant_id", "i.id", "i.created_at", "i.status", "'appName',i.agent_app_name,'channelType',i.channel_type,'attemptCount',i.attempt_count,'maxAttempts',i.max_attempts,'updatedAt',i.updated_at,'leaseVersion',i.lease_version::text,'traceId'," + traceIDSQL("i.trace_parent"), true},
	"executions":  {"execution_records e", "e.tenant_id", "e.id", "e.started_at", "e.status", "'appId',e.agent_app_id,'versionId',e.agent_version_id,'deploymentId',e.deployment_id,'attemptNumber',e.attempt_number,'startedAt',e.started_at,'completedAt',e.completed_at,'retrySafe',e.retry_safe", true},
	"outbox":      {"outbox_messages o", "o.tenant_id", "o.id", "o.created_at", "o.status", "'inboxId',o.inbox_id::text,'channelType',o.channel_type,'attemptCount',o.attempt_count,'maxAttempts',o.max_attempts,'updatedAt',o.updated_at,'deliveredAt',o.delivered_at,'leaseVersion',o.lease_version::text,'traceId'," + traceIDSQL("o.trace_parent"), true},
	"audit":       {"control_plane_audit a", "a.tenant_id", "a.id", "a.created_at", "a.action", "'action',a.action,'actor',a.actor,'resourceType',a.resource_type,'resourceId',a.resource_id", true},
	"migrations":  {"data_migrations m", "m.tenant_id", "m.id", "m.created_at", "m.phase", "'domain',m.domain,'phase',m.phase,'paused',m.paused,'updatedAt',m.updated_at", false},
}

func traceIDSQL(column string) string {
	return fmt.Sprintf("CASE WHEN %s ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$' AND substring(%s from 4 for 32) <> repeat('0',32) AND substring(%s from 37 for 16) <> repeat('0',16) THEN substring(%s from 4 for 32) ELSE '' END", column, column, column, column)
}

type reader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func list(ctx context.Context, db reader, view string, q query, extra string, extraArgs []any) (Page, error) {
	spec := views[view]
	args := []any{q.tenant}
	where := spec.tenant + "=$1"
	if q.status != "" {
		args = append(args, q.status)
		where += fmt.Sprintf(" AND %s=$%d", spec.status, len(args))
	}
	if q.after != nil {
		args = append(args, q.after.Time, q.after.ID)
		cast := "text"
		if spec.numeric {
			cast = "bigint"
		}
		where += fmt.Sprintf(" AND (%s,%s)<($%d::timestamptz,$%d::%s)", spec.timestamp, spec.id, len(args)-1, len(args), cast)
	}
	// detail's extra fragments are fixed SQL and use arguments after tenant.
	if extra != "" {
		where += " AND (" + extra + ")"
		args = append(args, extraArgs...)
	}
	args = append(args, q.limit+1)
	projection := fmt.Sprintf("json_build_object('id',%s::text,'tenantId',%s,'status',%s,'createdAt',%s,%s)", spec.id, spec.tenant, spec.status, spec.timestamp, spec.fields)
	statement := fmt.Sprintf("SELECT %s::text,%s,%s FROM %s WHERE %s ORDER BY %s DESC,%s DESC LIMIT $%d", spec.id, spec.timestamp, projection, spec.from, where, spec.timestamp, spec.id, len(args))
	rows, err := db.QueryContext(ctx, statement, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	page := Page{Items: []Item{}}
	var lastTime time.Time
	var lastID string
	for rows.Next() {
		var id string
		var stamp time.Time
		var raw []byte
		var item Item
		if err := rows.Scan(&id, &stamp, &raw); err != nil {
			return Page{}, err
		}
		if len(page.Items) == q.limit {
			page.NextCursor = cursorFor(view, q, lastTime, lastID)
			break
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return Page{}, err
		}
		page.Items = append(page.Items, item)
		lastTime, lastID = stamp, id
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}
	return page, nil
}

func (h *handler) overview(ctx context.Context, tenant string) (Overview, error) {
	result := Overview{}
	tx, err := h.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT
	(SELECT count(*) FROM agent_apps WHERE tenant_id=$1),
	(SELECT count(*) FROM agent_versions v JOIN agent_apps a ON a.id=v.agent_app_id WHERE a.tenant_id=$1),
	(SELECT count(*) FROM deployments WHERE tenant_id=$1),
	(SELECT count(*) FROM inbox_messages WHERE tenant_id=$1),
	(SELECT count(*) FROM outbox_messages WHERE tenant_id=$1),
	(SELECT count(*) FROM execution_records WHERE tenant_id=$1)`, tenant).Scan(&result.Counts.Apps, &result.Counts.Versions, &result.Counts.Deployments, &result.Counts.Inbox, &result.Counts.Outbox, &result.Counts.Executions)
	if err != nil {
		return result, err
	}
	for _, entry := range []struct {
		table string
		dest  *[]StateCount
	}{{"inbox_messages", &result.InboxStates}, {"outbox_messages", &result.OutboxStates}, {"execution_records", &result.ExecutionStates}} {
		*entry.dest = []StateCount{}
		rows, err := tx.QueryContext(ctx, "SELECT status,count(*) FROM "+entry.table+" WHERE tenant_id=$1 GROUP BY status ORDER BY status", tenant)
		if err != nil {
			return result, err
		}
		for rows.Next() {
			var item StateCount
			if err = rows.Scan(&item.Status, &item.Count); err != nil {
				rows.Close()
				return result, err
			}
			*entry.dest = append(*entry.dest, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return result, err
		}
	}
	return result, tx.Commit()
}

func (h *handler) detail(ctx context.Context, tenant, id string) (RequestDetail, error) {
	result := RequestDetail{Executions: []Item{}, Outbox: []Item{}, Audit: []Item{}}
	tx, err := h.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	q := query{tenant: tenant, limit: 100}
	inbox, err := list(ctx, tx, "inbox", q, "i.id=$2::bigint", []any{id})
	if err != nil {
		return result, err
	}
	if len(inbox.Items) == 0 {
		return result, sql.ErrNoRows
	}
	result.Inbox = inbox.Items[0]
	// Consumer persists this exact request identity; timestamps and session
	// coincidences are deliberately not association criteria.
	executions, err := list(ctx, tx, "executions", q, "e.idempotency_key=$2", []any{"inbox:" + id})
	if err != nil {
		return result, err
	}
	outbox, err := list(ctx, tx, "outbox", q, "o.inbox_id=$2::bigint", []any{id})
	if err != nil {
		return result, err
	}
	audit, err := list(ctx, tx, "audit", q, `
	(a.resource_type='inbox' AND a.resource_id=$2)
	OR (a.resource_type='execution' AND EXISTS (SELECT 1 FROM execution_records e WHERE e.tenant_id=$1 AND e.idempotency_key=$3 AND e.id::text=a.resource_id))
	OR (a.resource_type='outbox' AND EXISTS (SELECT 1 FROM outbox_messages o WHERE o.tenant_id=$1 AND o.inbox_id=$2::bigint AND o.id::text=a.resource_id))`, []any{id, "inbox:" + id})
	if err != nil {
		return result, err
	}
	result.Executions, result.Outbox, result.Audit = executions.Items, outbox.Items, audit.Items
	result.Truncated = executions.NextCursor != "" || outbox.NextCursor != "" || audit.NextCursor != ""
	return result, tx.Commit()
}
