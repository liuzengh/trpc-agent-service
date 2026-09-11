package memorystore

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

func migrationCandidate(scope Scope, snapshot Snapshot, capacity int) (Candidate, []byte, string, error) {
	base := uint64(0)
	if snapshot.Revision > 0 {
		base = snapshot.Revision - 1
	}
	candidate := Candidate{Scope: scope, BaseRevision: base, Entries: snapshot.Entries}
	body, err := candidate.encode()
	if err != nil {
		return Candidate{}, nil, "", err
	}
	if len(body) > capacity {
		return Candidate{}, nil, "", ErrCapacity
	}
	hash, err := SnapshotDigest(scope, snapshot)
	if err != nil {
		return Candidate{}, nil, "", err
	}
	return candidate, body, hash, nil
}

// ImportSnapshot writes an exact current snapshot only into an empty target.
// Replaying the identical import succeeds; different existing state conflicts.
// The caller owns write quiescence and cutover ordering.
func (s *Postgres) ImportSnapshot(ctx context.Context, scope Scope, snapshot Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, body, hash, err := migrationCandidate(scope, snapshot, s.capacityBytes)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storageFailure(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, scope.TenantID+"\x1f"+scope.ID); err != nil {
		return storageFailure(err)
	}
	var revision int64
	var existing []byte
	var existingDigest string
	err = tx.QueryRow(ctx, `SELECT revision,content,digest FROM runtime_memory.memory_heads WHERE tenant_id=$1 AND scope_id=$2 FOR UPDATE`, scope.TenantID, scope.ID).Scan(&revision, &existing, &existingDigest)
	if err == nil {
		if uint64(revision) != snapshot.Revision || existingDigest != hash || string(existing) != string(body) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return storageFailure(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO runtime_memory.memory_heads(tenant_id,scope_id,revision,content,digest) VALUES($1,$2,$3,$4,$5)`, scope.TenantID, scope.ID, int64(snapshot.Revision), body, hash); err != nil {
		return storageFailure(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return storageFailure(err)
	}
	return nil
}

const redisImportScript = `
local typ=redis.call('TYPE',KEYS[1]).ok
if typ~='none' and typ~='string' then return 'corrupt' end
if redis.call('PTTL',KEYS[1])>=0 then return 'corrupt' end
local old=redis.call('GET',KEYS[1])
if old then
 if old==ARGV[1] then return 'ok' end
 return 'conflict'
end
redis.call('MSET',KEYS[1],ARGV[1])
return 'ok'
`

func (s *Redis) ImportSnapshot(ctx context.Context, scope Scope, snapshot Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, body, hash, err := migrationCandidate(scope, snapshot, s.capacity)
	if err != nil {
		return err
	}
	key := redisKey(scope, "head", scope.ID)
	if snapshot.Revision == 0 {
		exists, err := s.client.Exists(ctx, key).Result()
		if err != nil {
			return redisFailure(err)
		}
		if exists != 0 {
			return ErrConflict
		}
		return nil
	}
	record := redisRecord{
		Revision:     strconv.FormatUint(snapshot.Revision, 10),
		CompletionID: "migration-" + hash[7:39],
		RunID:        "migration-" + hash[39:], AttemptID: "migration-import",
		Digest: hash, Body: string(body),
	}
	encoded, _ := json.Marshal(record)
	result, err := s.client.Eval(ctx, redisImportScript, []string{key}, string(encoded)).Text()
	if err != nil {
		return redisFailure(err)
	}
	switch result {
	case "ok":
		return nil
	case "conflict":
		return ErrConflict
	default:
		return ErrCorrupt
	}
}
