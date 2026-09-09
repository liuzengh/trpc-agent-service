package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"time"
)

type sessionLister interface {
	ListSessionIDs(context.Context, string) ([]string, error)
}
type MigrationOptions struct {
	TenantID       string
	DryRun         bool
	BatchSize      int
	CheckpointPath string
	MaxRetries     int
	Progress       func(MigrationReport)
	Cutover        func(context.Context) error
}
type MigrationReport struct {
	Status            string `json:"status"`
	DryRun            bool   `json:"dry_run"`
	Sessions          int    `json:"sessions"`
	ProcessedSessions int    `json:"processed_sessions"`
	SourceCount       int    `json:"source_count"`
	DestinationCount  int    `json:"destination_count"`
	Checksum          string `json:"checksum"`
	Resumed           bool   `json:"resumed"`
	Matched           bool   `json:"matched"`
	Message           string `json:"message,omitempty"`
}
type migrationCheckpoint struct {
	TenantID string `json:"tenant_id"`
	Next     int    `json:"next"`
	Checksum string `json:"checksum,omitempty"`
}

// MigrateRedisToSQL copies a quiescent tenant. Callers must stop writers on both
// backends until validation and routing cutover have completed.
func MigrateRedisToSQL(ctx context.Context, source DataStore, destination DataStore, options MigrationOptions) (report MigrationReport, err error) {
	report = MigrationReport{Status: "running", DryRun: options.DryRun}
	defer func() {
		if err != nil {
			report.Status = "failed"
			report.Message = "migration failed"
		}
	}()
	if options.TenantID == "" {
		return report, errors.New("tenant is required")
	}
	var sourceFreeze *RedisStore
	var freezeToken string
	if redisSource, ok := source.(*RedisStore); ok && !options.DryRun {
		token, freezeErr := redisSource.freezeMigration(ctx, options.TenantID)
		if freezeErr != nil {
			return report, freezeErr
		}
		sourceFreeze, freezeToken = redisSource, token
		defer func() {
			if sourceFreeze != nil {
				releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if releaseErr := sourceFreeze.unfreezeMigration(releaseCtx, options.TenantID, freezeToken); releaseErr != nil && err == nil {
					err = releaseErr
				}
			}
		}()
	}
	ctx = context.WithValue(ctx, migrationImportKey{}, true)
	before, err := snapshotMigration(ctx, source, options.TenantID)
	if err != nil {
		return report, err
	}
	report.Sessions, report.SourceCount, report.Checksum = len(before.sessions), before.count, before.checksum
	target, err := snapshotMigration(ctx, destination, options.TenantID)
	if err != nil {
		return report, err
	}
	report.DestinationCount = target.count
	report.Matched = before.checksum == target.checksum
	if options.DryRun {
		report.Status = "completed"
		return report, nil
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	if options.MaxRetries <= 0 {
		options.MaxRetries = 3
	}
	checkpoint := migrationCheckpoint{TenantID: options.TenantID, Checksum: before.checksum}
	if options.CheckpointPath != "" {
		data, readErr := os.ReadFile(options.CheckpointPath)
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return report, readErr
		}
		if readErr == nil {
			if err := json.Unmarshal(data, &checkpoint); err != nil {
				return report, err
			}
			if checkpoint.TenantID != options.TenantID || checkpoint.Next < 0 || checkpoint.Next > len(before.sessions) || (checkpoint.Checksum != "" && checkpoint.Checksum != before.checksum) {
				return report, errors.New("checkpoint does not match source snapshot")
			}
			report.Resumed = checkpoint.Next > 0
		}
	}
	// Revalidate every batch on resume by idempotent replay. A cursor alone cannot
	// prove that this is the same destination or that an earlier batch still exists.
	checkpoint.Checksum = before.checksum
	for index, session := range before.sessions {
		for _, event := range before.events[session] {
			if err := retry(ctx, options.MaxRetries, func() error { return destination.AppendSessionEvent(ctx, event) }); err != nil {
				return report, err
			}
		}
		for _, item := range before.memory[session] {
			if err := retry(ctx, options.MaxRetries, func() error { return destination.PutMemory(ctx, item) }); err != nil {
				return report, err
			}
		}
		report.ProcessedSessions = index + 1
		if (index+1)%options.BatchSize == 0 || index+1 == len(before.sessions) {
			checkpoint.Next = index + 1
			if err := writeCheckpoint(options.CheckpointPath, checkpoint); err != nil {
				return report, err
			}
			if options.Progress != nil {
				options.Progress(report)
			}
		}
	}
	if len(before.artifacts) > 0 {
		store, ok := destination.(ArtifactStore)
		if !ok {
			return report, errors.New("destination does not support artifacts")
		}
		for _, item := range before.artifacts {
			if err := retry(ctx, options.MaxRetries, func() error { _, err := store.PutArtifact(ctx, item); return err }); err != nil {
				return report, err
			}
		}
	}
	if len(before.knowledge) > 0 {
		store, ok := destination.(KnowledgeStore)
		if !ok {
			return report, errors.New("destination does not support knowledge")
		}
		for _, item := range before.knowledge {
			if err := retry(ctx, options.MaxRetries, func() error { _, err := store.PutKnowledge(ctx, item); return err }); err != nil {
				return report, err
			}
		}
	}
	after, err := snapshotMigration(ctx, source, options.TenantID)
	if err != nil {
		return report, err
	}
	if after.checksum != before.checksum {
		return report, errors.New("source changed during migration; stop writers and retry")
	}
	target, err = snapshotMigration(ctx, destination, options.TenantID)
	if err != nil {
		return report, err
	}
	report.DestinationCount = target.count
	report.Matched = target.checksum == before.checksum
	if !report.Matched {
		return report, errors.New("source and destination content differ")
	}
	if options.CheckpointPath != "" {
		if err := os.Remove(options.CheckpointPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return report, err
		}
	}
	if options.Cutover != nil {
		if err := options.Cutover(ctx); err != nil {
			return report, err
		}
		// Keep the retired Redis source read-only, including already acquired handles.
		sourceFreeze = nil
	}
	report.Status = "completed"
	return report, nil
}

type migrationSnapshot struct {
	sessions  []string
	events    map[string][]SessionEvent
	memory    map[string][]MemoryRecord
	artifacts []Artifact
	knowledge []KnowledgeRecord
	count     int
	checksum  string
}

func snapshotMigration(ctx context.Context, store DataStore, tenant string) (migrationSnapshot, error) {
	result := migrationSnapshot{events: map[string][]SessionEvent{}, memory: map[string][]MemoryRecord{}}
	lister, ok := store.(sessionLister)
	if !ok {
		return result, errors.New("backend does not support enumeration")
	}
	sessions, err := lister.ListSessionIDs(ctx, tenant)
	if err != nil {
		return result, err
	}
	sort.Strings(sessions)
	result.sessions = sessions
	hash := sha256.New()
	writeChecksumField(hash, []byte("tenant-data-v2"))
	for _, session := range sessions {
		events, memory, err := readSessionData(ctx, store, tenant, session)
		if err != nil {
			return result, err
		}
		result.events[session], result.memory[session] = events, memory
		result.count += len(events) + len(memory)
		writeChecksumField(hash, []byte(session))
		writeChecksumField(hash, []byte(migrationChecksum(events, memory)))
	}
	if rich, ok := store.(ArtifactStore); ok {
		result.artifacts, err = rich.ListArtifacts(ctx, tenant, "")
		if err != nil {
			return result, err
		}
	}
	if rich, ok := store.(KnowledgeStore); ok {
		result.knowledge, err = rich.ListKnowledge(ctx, tenant, "")
		if err != nil {
			return result, err
		}
	}
	sort.Slice(result.artifacts, func(i, j int) bool { return result.artifacts[i].ID < result.artifacts[j].ID })
	sort.Slice(result.knowledge, func(i, j int) bool { return result.knowledge[i].ID < result.knowledge[j].ID })
	for _, item := range result.artifacts {
		data, err := json.Marshal(item)
		if err != nil {
			return result, err
		}
		writeChecksumField(hash, data)
	}
	for _, item := range result.knowledge {
		data, err := json.Marshal(item)
		if err != nil {
			return result, err
		}
		writeChecksumField(hash, data)
	}
	result.count += len(result.artifacts) + len(result.knowledge)
	result.checksum = hex.EncodeToString(hash.Sum(nil))
	return result, nil
}
func readSessionData(ctx context.Context, store DataStore, tenant, session string) ([]SessionEvent, []MemoryRecord, error) {
	events, err := store.ListSessionEvents(ctx, tenant, session, 0)
	if err != nil {
		return nil, nil, err
	}
	memory, err := store.ListMemory(ctx, tenant, session)
	return events, memory, err
}
func retry(ctx context.Context, max int, fn func() error) error {
	var err error
	for i := 0; i < max; i++ {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(i+1) * 10 * time.Millisecond):
		}
	}
	return err
}
func writeCheckpoint(path string, value migrationCheckpoint) error {
	if path == "" {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (s *InMemoryStore) ListSessionIDs(ctx context.Context, tenant string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := map[string]struct{}{}
	prefix := tenant + "\x00"
	for key := range s.events {
		if strings.HasPrefix(key, prefix) {
			set[strings.TrimPrefix(key, prefix)] = struct{}{}
		}
	}
	for key := range s.memory {
		if strings.HasPrefix(key, prefix) {
			remainder := strings.TrimPrefix(key, prefix)
			if index := strings.IndexByte(remainder, '\x00'); index >= 0 {
				set[remainder[:index]] = struct{}{}
			}
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}
func (s *SQLStore) ListSessionIDs(ctx context.Context, tenant string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT session_id FROM session_events WHERE tenant_id=? UNION SELECT session_id FROM session_memory WHERE tenant_id=? ORDER BY session_id`), tenant, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (s *RedisStore) ListSessionIDs(ctx context.Context, tenant string) ([]string, error) {
	ids := map[string]struct{}{}
	for _, kind := range []string{"events", "memory"} {
		prefix := s.prefix + kind + ":" + tenant + ":"
		var cursor uint64
		for {
			keys, next, err := s.client.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				return nil, err
			}
			for _, key := range keys {
				ids[strings.TrimPrefix(key, prefix)] = struct{}{}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}
