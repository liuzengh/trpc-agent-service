package memorystore

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func (s *Postgres) Load(ctx context.Context, scope Scope) (Snapshot, error) {
	if !scope.valid() {
		return Snapshot{}, ErrIdentity
	}
	var rev int64
	var body []byte
	var hash string
	err := s.pool.QueryRow(ctx, `SELECT revision,content,digest FROM runtime_memory.memory_heads WHERE tenant_id=$1 AND scope_id=$2`, scope.TenantID, scope.ID).Scan(&rev, &body, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return Snapshot{Entries: []*memory.Entry{}}, nil
	}
	if err != nil {
		return Snapshot{}, storageFailure(err)
	}
	c, err := decode(body, scope, hash, s.capacityBytes)
	if err != nil {
		return Snapshot{}, storageFailure(err)
	}
	if rev < 0 || (rev > 0 && c.BaseRevision+1 != uint64(rev)) {
		return Snapshot{}, ErrCorrupt
	}
	return Snapshot{Revision: uint64(rev), Entries: c.Entries}, nil
}
func (s *Postgres) ApplyAccepted(ctx context.Context, a Accepted, c Candidate) (Snapshot, error) {
	if !validID(a.CompletionID) || !validID(a.RunID) || !validID(a.AttemptID) {
		return Snapshot{}, ErrIdentity
	}
	body, err := c.encode()
	if err != nil {
		return Snapshot{}, storageFailure(err)
	}
	if len(body) > s.capacityBytes {
		return Snapshot{}, ErrCapacity
	}
	if digest(body) != a.CandidateDigest {
		return Snapshot{}, ErrIdentity
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Snapshot{}, storageFailure(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	// Completion lock also serializes cross-scope reuse before checking receipt.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, c.Scope.TenantID+"\x1f"+a.CompletionID); err != nil {
		return Snapshot{}, storageFailure(err)
	}
	var oldScope, oldRun, oldAttempt, oldHash string
	var rev int64
	var oldBody []byte
	err = tx.QueryRow(ctx, `SELECT scope_id,run_id,attempt_id,candidate_digest,applied_revision,content FROM runtime_memory.memory_receipts WHERE tenant_id=$1 AND completion_id=$2`, c.Scope.TenantID, a.CompletionID).Scan(&oldScope, &oldRun, &oldAttempt, &oldHash, &rev, &oldBody)
	if err == nil {
		if oldScope != c.Scope.ID || oldRun != a.RunID || oldAttempt != a.AttemptID || oldHash != a.CandidateDigest {
			return Snapshot{}, ErrConflict
		}
		saved, e := decode(oldBody, c.Scope, oldHash, s.capacityBytes)
		if e != nil || rev <= 0 || uint64(rev) != saved.BaseRevision+1 {
			return Snapshot{}, ErrCorrupt
		}
		return Snapshot{Revision: uint64(rev), Entries: saved.Entries}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Snapshot{}, storageFailure(err)
	}
	empty := Candidate{Scope: c.Scope}
	initial, _ := empty.encode()
	if _, err = tx.Exec(ctx, `INSERT INTO runtime_memory.memory_heads(tenant_id,scope_id,revision,content,digest) VALUES($1,$2,0,$3,$4) ON CONFLICT DO NOTHING`, c.Scope.TenantID, c.Scope.ID, initial, digest(initial)); err != nil {
		return Snapshot{}, storageFailure(err)
	}
	if err = tx.QueryRow(ctx, `SELECT revision FROM runtime_memory.memory_heads WHERE tenant_id=$1 AND scope_id=$2 FOR UPDATE`, c.Scope.TenantID, c.Scope.ID).Scan(&rev); err != nil {
		return Snapshot{}, storageFailure(err)
	}
	if uint64(rev) != c.BaseRevision {
		return Snapshot{}, ErrConflict
	}
	next := rev + 1
	if _, err = tx.Exec(ctx, `UPDATE runtime_memory.memory_heads SET revision=$3,content=$4,digest=$5 WHERE tenant_id=$1 AND scope_id=$2`, c.Scope.TenantID, c.Scope.ID, next, body, a.CandidateDigest); err != nil {
		return Snapshot{}, storageFailure(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO runtime_memory.memory_receipts(tenant_id,completion_id,run_id,attempt_id,scope_id,candidate_digest,applied_revision,content) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, c.Scope.TenantID, a.CompletionID, a.RunID, a.AttemptID, c.Scope.ID, a.CandidateDigest, next, body); err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == "23505" {
			return Snapshot{}, ErrConflict
		}
		return Snapshot{}, storageFailure(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Snapshot{}, storageFailure(err)
	}
	saved, err := decode(body, c.Scope, a.CandidateDigest, s.capacityBytes)
	if err != nil {
		return Snapshot{}, storageFailure(err)
	}
	return Snapshot{Revision: uint64(next), Entries: saved.Entries}, nil
}

// Driver diagnostics may include connection parameters or data values.
func storageFailure(err error) error {
	for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded, ErrIdentity, ErrConflict, ErrCorrupt, ErrCapacity, ErrPreparation} {
		if errors.Is(err, sentinel) {
			return sentinel
		}
	}
	return ErrUnavailable
}
