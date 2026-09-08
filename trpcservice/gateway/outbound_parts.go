package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

var ErrPartConflict = errors.New("outbound part ownership or input conflict")

type OutboundPart struct {
	TenantID, OutboundID                 string
	Index, Total                         int
	InputHash, Owner, Status, ProviderID string
}
type PartJournal interface {
	ListParts(context.Context, string, string) ([]OutboundPart, error)
	ReconcilePart(context.Context, OutboundPart, string, string, string, string, string) error
	RenewOutbound(context.Context, OutboundItem, string, time.Duration) error
	BeginPart(context.Context, OutboundItem, string, int, int, string) (OutboundPart, bool, error)
	FinishPart(context.Context, OutboundPart, string, string) error
}

func (j *MemoryJournal) ListParts(ctx context.Context, t, id string) ([]OutboundPart, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	out := []OutboundPart{}
	for _, p := range j.parts {
		if p.TenantID == t && p.OutboundID == id {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}
func (j *PostgresJournal) ListParts(ctx context.Context, t, id string) ([]OutboundPart, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT part_index,total_parts,input_hash,owner,status,provider_message_id FROM outbound_part WHERE tenant_id=$1 AND outbound_id=$2 ORDER BY part_index LIMIT 10000`, t, id)
	if err != nil {
		return nil, err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(rows)
	out := []OutboundPart{}
	for rows.Next() {
		p := OutboundPart{TenantID: t, OutboundID: id}
		if err := rows.Scan(&p.Index, &p.Total, &p.InputHash, &p.Owner, &p.Status, &p.ProviderID); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (j *MemoryJournal) ReconcilePart(ctx context.Context, p OutboundPart, outcome, provider, evidence, actor, trace string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if (outcome != "sent" && outcome != "not_sent") || len(evidence) != 64 {
		return ErrPartConflict
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	parent := j.outbound[p.OutboundID]
	key := partKey(p.TenantID, p.OutboundID, p.Index)
	old, ok := j.parts[key]
	if !ok || parent == nil || parent.item.TenantID != p.TenantID || parent.status == "sent" || parent.lockedUntil.After(time.Now()) || old.Owner != p.Owner || (old.Status != "unknown" && old.Status != "attempting") {
		return ErrPartConflict
	}
	old.Status = "pending"
	old.ProviderID = ""
	if outcome == "sent" {
		old.Status = "sent"
		old.ProviderID = provider
	}
	j.parts[key] = old
	n := 0
	for _, item := range j.parts {
		if item.TenantID == p.TenantID && item.OutboundID == p.OutboundID && item.Status == "sent" {
			n++
		}
	}
	parent.status = "pending"
	parent.nextAttempt = time.Now()
	parent.lockedBy = ""
	parent.lockedUntil = time.Time{}
	if n == old.Total {
		parent.status = "sent"
	}
	return nil
}
func (j *PostgresJournal) ReconcilePart(ctx context.Context, p OutboundPart, outcome, provider, evidence, actor, trace string) error {
	var accepted bool
	err := j.db.QueryRowContext(ctx, "SELECT platform_reconcile_outbound_part($1,$2,$3,$4,$5,$6,$7,$8,$9)", p.TenantID, p.OutboundID, p.Index, p.Owner, outcome, provider, evidence, actor, trace).Scan(&accepted)
	if err != nil {
		return err
	}
	if !accepted {
		return ErrPartConflict
	}
	return nil
}

func validPart(index, total int, hash string) bool {
	return index >= 0 && total > 0 && index < total && total <= 10000 && len(hash) == 64
}
func partKey(t, id string, index int) string { return stableID("part_", t, id, fmt.Sprint(index)) }

func (j *MemoryJournal) BeginPart(ctx context.Context, item OutboundItem, worker string, index, total int, hash string) (OutboundPart, bool, error) {
	if err := ctx.Err(); err != nil {
		return OutboundPart{}, false, err
	}
	if !validPart(index, total, hash) {
		return OutboundPart{}, false, ErrPartConflict
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	parent := j.outbound[item.ID]
	if j.closed || parent == nil || parent.item.TenantID != item.TenantID || parent.lockedBy != worker || parent.item.AttemptCount != item.AttemptCount || parent.status != "sending" || !parent.lockedUntil.After(time.Now()) {
		return OutboundPart{}, false, ErrPartConflict
	}
	if j.parts == nil {
		j.parts = map[string]OutboundPart{}
	}
	key := partKey(item.TenantID, item.ID, index)
	p, ok := j.parts[key]
	if ok {
		if p.InputHash != hash || p.Total != total {
			return p, false, ErrPartConflict
		}
		if p.Status != "pending" {
			return p, false, nil
		}
	}
	p = OutboundPart{TenantID: item.TenantID, OutboundID: item.ID, Index: index, Total: total, InputHash: hash, Owner: uuid.NewString(), Status: "attempting"}
	j.parts[key] = p
	return p, true, nil
}
func (j *MemoryJournal) FinishPart(ctx context.Context, p OutboundPart, status, providerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validPartStatus(status) {
		return ErrPartConflict
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	key := partKey(p.TenantID, p.OutboundID, p.Index)
	old, ok := j.parts[key]
	if !ok || old.Owner != p.Owner || old.InputHash != p.InputHash || old.Status != "attempting" {
		return ErrPartConflict
	}
	old.Status = status
	old.ProviderID = providerID
	j.parts[key] = old
	return nil
}
func validPartStatus(s string) bool {
	return s == "pending" || s == "sent" || s == "rejected" || s == "unknown"
}

func (j *PostgresJournal) BeginPart(ctx context.Context, item OutboundItem, worker string, index, total int, hash string) (OutboundPart, bool, error) {
	if !validPart(index, total, hash) {
		return OutboundPart{}, false, ErrPartConflict
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return OutboundPart{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	var owns bool
	err = tx.QueryRowContext(ctx, `SELECT tenant_id=$2 AND delivery_protocol=1 AND status='sending' AND locked_by=$3 AND attempt_count=$4 AND locked_until>now() FROM outbound_message WHERE outbound_id=$1 FOR UPDATE`, item.ID, item.TenantID, worker, item.AttemptCount).Scan(&owns)
	if err != nil || !owns {
		return OutboundPart{}, false, ErrPartConflict
	}
	p := OutboundPart{TenantID: item.TenantID, OutboundID: item.ID, Index: index, Total: total, InputHash: hash}
	err = tx.QueryRowContext(ctx, `SELECT total_parts,input_hash,owner,status,provider_message_id FROM outbound_part WHERE tenant_id=$1 AND outbound_id=$2 AND part_index=$3 FOR UPDATE`, item.TenantID, item.ID, index).Scan(&p.Total, &p.InputHash, &p.Owner, &p.Status, &p.ProviderID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return p, false, err
	}
	if err == nil {
		if p.Total != total || p.InputHash != hash {
			return p, false, ErrPartConflict
		}
		if p.Status != "pending" {
			return p, false, tx.Commit()
		}
	}
	p.Owner = uuid.NewString()
	p.Status = "attempting"
	_, err = tx.ExecContext(ctx, `INSERT INTO outbound_part(tenant_id,outbound_id,part_index,total_parts,input_hash,owner,status) VALUES($1,$2,$3,$4,$5,$6,'attempting') ON CONFLICT(tenant_id,outbound_id,part_index) DO UPDATE SET owner=EXCLUDED.owner,status='attempting',updated_at=now()`, p.TenantID, p.OutboundID, p.Index, p.Total, p.InputHash, p.Owner)
	if err != nil {
		return p, false, err
	}
	return p, true, tx.Commit()
}

func (j *MemoryJournal) RenewOutbound(ctx context.Context, item OutboundItem, worker string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	p := j.outbound[item.ID]
	if p == nil || p.item.TenantID != item.TenantID || p.item.AttemptCount != item.AttemptCount || p.status != "sending" || p.lockedBy != worker || !p.lockedUntil.After(time.Now()) {
		return ErrPartConflict
	}
	p.lockedUntil = time.Now().Add(ttl)
	return nil
}
func (j *PostgresJournal) RenewOutbound(ctx context.Context, item OutboundItem, worker string, ttl time.Duration) error {
	result, err := j.db.ExecContext(ctx, `UPDATE outbound_message SET locked_until=now()+$5::interval WHERE outbound_id=$1 AND tenant_id=$2 AND locked_by=$3 AND attempt_count=$4 AND status='sending' AND locked_until>now()`, item.ID, item.TenantID, worker, item.AttemptCount, postgresInterval(ttl))
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrPartConflict
	}
	return nil
}
func (j *PostgresJournal) FinishPart(ctx context.Context, p OutboundPart, status, providerID string) error {
	if !validPartStatus(status) {
		return ErrPartConflict
	}
	result, err := j.db.ExecContext(ctx, `UPDATE outbound_part SET status=$5,provider_message_id=$6,updated_at=now() WHERE tenant_id=$1 AND outbound_id=$2 AND part_index=$3 AND owner=$4 AND status='attempting' AND input_hash=$7`, p.TenantID, p.OutboundID, p.Index, p.Owner, status, providerID, p.InputHash)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrPartConflict
	}
	return nil
}
