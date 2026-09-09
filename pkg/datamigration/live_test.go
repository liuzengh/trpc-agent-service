package datamigration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type liveTestBackend struct {
	info    LiveBackendInfo
	values  map[string][]byte
	applied []Record
	corrupt bool
}

func (b *liveTestBackend) MigrationInfo() LiveBackendInfo { return b.info }

type liveRepairBackend struct {
	*liveTestBackend
	repairErr error
	repairs   int
}

func (b *liveRepairBackend) ReconcileSource(context.Context, string) error {
	b.repairs++
	return b.repairErr
}

func TestLiveSourceCleanupFailureStopsCaptureButTargetReadDoesNotRepair(t *testing.T) {
	coordinator, _, source, _ := newLiveTest(t)
	failure := errors.New("object cleanup interrupted")
	backend := &liveRepairBackend{liveTestBackend: source, repairErr: failure}
	if _, err := coordinator.captureKey(context.Background(), &liveAccess{source: backend}, "deleted-version"); !errors.Is(err, failure) {
		t.Fatalf("cleanup failure was not preserved before journal capture: %v", err)
	}
	if backend.repairs != 1 {
		t.Fatal("source cleanup was not attempted")
	}
	if _, err := readLiveRecord(context.Background(), backend, "deleted-version", 0); err != nil {
		t.Fatal(err)
	}
	if backend.repairs != 1 {
		t.Fatal("verification reads must not repair a target")
	}
}
func (b *liveTestBackend) Keys(context.Context, string, int) ([]string, string, bool, error) {
	var keys []string
	for key := range b.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, "", true, nil
}
func (b *liveTestBackend) Read(_ context.Context, key string, version int64) (Record, error) {
	payload, exists := b.values[key]
	if !exists {
		return LiveTombstone(key, version), nil
	}
	if b.corrupt {
		payload = []byte("different committed target")
	}
	digest := sha256.Sum256(payload)
	return Record{Key: key, Version: version, Hash: hex.EncodeToString(digest[:]), Payload: append([]byte(nil), payload...)}, nil
}
func (b *liveTestBackend) Apply(_ context.Context, record Record) error {
	b.applied = append(b.applied, record)
	if record.Deleted {
		delete(b.values, record.Key)
	} else {
		b.values[record.Key] = append([]byte(nil), record.Payload...)
	}
	return nil
}

func newLiveTest(t *testing.T) (*LiveCoordinator, sqlmock.Sqlmock, *liveTestBackend, *liveTestBackend) {
	t.Helper()
	dsn := "live-migration-" + t.Name()
	db, mock, err := sqlmock.NewWithDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	gateDB, err := sql.Open("sqlmock", dsn)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	gateDB.SetMaxOpenConns(8)
	source := &liveTestBackend{info: LiveBackendInfo{Backend: "redis", Identity: "source-db", Compatibility: "session/v1"}, values: map[string][]byte{}}
	target := &liveTestBackend{info: LiveBackendInfo{Backend: "postgres", Identity: "target-db", Compatibility: "session/v1"}, values: map[string][]byte{}}
	coordinator, err := NewLiveCoordinator(LiveOptions{DB: db, GateDB: gateDB, Owner: "owner", Resolve: func(_ context.Context, tenantID string, domain Domain, profile string) (LiveBackend, func(), error) {
		if tenantID != "tenant-a" || domain != DomainSession {
			return nil, nil, ErrInvalidMigration
		}
		switch profile {
		case "source":
			return source, func() {}, nil
		case "target":
			return target, func() {}, nil
		default:
			return nil, nil, ErrMigrationCapability
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
		_ = gateDB.Close()
	})
	return coordinator, mock, source, target
}

func liveTestRoute() liveRoute {
	return liveRoute{JobID: "migration", TenantID: "tenant-a", Domain: DomainSession,
		SourceProfile: "source", TargetProfile: "target", SourceBackend: "redis", TargetBackend: "postgres",
		ReadProfile: "source", WriteProfile: "source", Mirroring: true, ConfigVersion: 1,
		SourceIdentity: "source-db", TargetIdentity: "target-db", Compatibility: "session/v1",
		ScanDone: true, Actor: "operator", Reason: "approved migration"}
}

func liveRouteRows(route liveRoute) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"migration_id", "tenant_id", "domain", "source_profile", "target_profile", "source_backend", "target_backend",
		"read_profile", "write_profile", "mirroring", "config_version", "scan_cursor", "scan_done", "actor", "reason",
		"source_identity", "target_identity", "compatibility"}).AddRow(
		route.JobID, route.TenantID, route.Domain, route.SourceProfile, route.TargetProfile, route.SourceBackend, route.TargetBackend,
		route.ReadProfile, route.WriteProfile, route.Mirroring, route.ConfigVersion, route.ScanCursor, route.ScanDone, route.Actor, route.Reason,
		route.SourceIdentity, route.TargetIdentity, route.Compatibility)
}

func expectLiveGate(mock sqlmock.Sqlmock, route liveRoute, shared bool) {
	query := "SELECT pg_advisory_lock("
	if shared {
		query = "SELECT pg_advisory_lock_shared("
	}
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("online-data-migration/v1/tenant-a/session").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM data_migration_live_routes r").WithArgs("tenant-a", DomainSession).WillReturnRows(liveRouteRows(route))
}

func expectLiveUnlock(mock sqlmock.Sqlmock, shared bool) {
	query := "SELECT pg_advisory_unlock("
	if shared {
		query = "SELECT pg_advisory_unlock_shared("
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("online-data-migration/v1/tenant-a/session").WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
}

func expectLiveActiveGate(mock sqlmock.Sqlmock, route liveRoute) {
	expectLiveGate(mock, route, true)
	expectLiveUnlock(mock, true)
	expectLiveGate(mock, route, false)
}

func liveRecordRows(records ...Record) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"record_key", "sequence", "content_hash", "payload", "deleted"})
	for _, record := range records {
		rows.AddRow(record.Key, record.Version, record.Hash, record.Payload, record.Deleted)
	}
	return rows
}

func expectLivePending(mock sqlmock.Sqlmock, records ...Record) {
	mock.ExpectQuery("WHERE migration_id=\\$1 AND projected_at IS NULL").WithArgs("migration", 128).WillReturnRows(liveRecordRows(records...))
}

func expectLiveClean(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT record_key FROM data_migration_live_intents").WithArgs("migration", 128).WillReturnRows(sqlmock.NewRows([]string{"record_key"}))
	expectLivePending(mock)
}

func expectLiveCapture(mock sqlmock.Sqlmock, record Record) {
	mock.ExpectQuery("WHERE migration_id=\\$1 AND key_hash=\\$2 ORDER BY sequence DESC LIMIT 1").
		WithArgs("migration", liveKeyHash(record.Key)).WillReturnRows(liveRecordRows())
	mock.ExpectQuery("INSERT INTO data_migration_live_journal").WithArgs("migration", liveKeyHash(record.Key), record.Key,
		record.Payload, record.Hash, record.Deleted).WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(record.Version))
}

func expectLiveProject(mock sqlmock.Sqlmock, record Record, mismatch bool) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (SELECT 1 FROM data_migration_live_journal")).
		WithArgs("migration", liveKeyHash(record.Key), record.Version).WillReturnRows(sqlmock.NewRows([]string{"projected"}).AddRow(false))
	if !mismatch {
		mock.ExpectExec("UPDATE data_migration_live_journal SET projected_at=clock_timestamp").
			WithArgs("migration", record.Version, liveKeyHash(record.Key), record.Hash, record.Deleted).WillReturnResult(sqlmock.NewResult(0, 1))
	}
}

func TestLiveCachedWriterUsesTerminalRoute(t *testing.T) {
	c, mock, _, _ := newLiveTest(t)
	route := liveTestRoute()
	route.Mirroring, route.ReadProfile, route.WriteProfile = false, "target", "target"
	expectLiveGate(mock, route, true)
	expectLiveUnlock(mock, true)
	called := false
	err := c.WithWrite(context.Background(), "tenant-a", DomainSession, "stale-cached-source", nil,
		func(_ context.Context, profile string) error {
			called = true
			if profile != "target" {
				t.Fatalf("cached worker selected %q", profile)
			}
			return nil
		})
	if err != nil || !called {
		t.Fatalf("write called=%v error=%v", called, err)
	}
}

func TestLiveUnsupportedMutationIsRejectedBeforeSourceWrite(t *testing.T) {
	c, mock, _, _ := newLiveTest(t)
	expectLiveActiveGate(mock, liveTestRoute())
	expectLiveUnlock(mock, false)
	err := c.WithWrite(context.Background(), "tenant-a", DomainSession, "source", nil,
		func(context.Context, string) error { t.Fatal("unsupported mutation ran"); return nil })
	if !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("got %v", err)
	}
}

func TestLiveSourceErrorAndCancellationRetainDirtyIntent(t *testing.T) {
	for _, sourceErr := range []error{errors.New("source result unknown"), context.Canceled} {
		t.Run(sourceErr.Error(), func(t *testing.T) {
			c, mock, source, target := newLiveTest(t)
			expectLiveActiveGate(mock, liveTestRoute())
			expectLiveClean(mock)
			mock.ExpectBegin()
			mock.ExpectQuery("INSERT INTO data_migration_live_intents").WithArgs("migration", liveKeyHash("key"), "key").
				WillReturnRows(sqlmock.NewRows([]string{"record_key"}).AddRow("key"))
			mock.ExpectCommit()
			source.values["key"] = []byte("committed despite error")
			record, _ := source.Read(context.Background(), "key", 1)
			expectLiveCapture(mock, record)
			expectLivePending(mock, record)
			expectLiveProject(mock, record, false)
			expectLivePending(mock)
			// No DELETE intent is expected after either kind of source error.
			expectLiveUnlock(mock, false)
			err := c.WithWrite(context.Background(), "tenant-a", DomainSession, "source", []string{"key"},
				func(context.Context, string) error { return sourceErr })
			if !errors.Is(err, sourceErr) || string(target.values["key"]) != "committed despite error" {
				t.Fatalf("error=%v target=%q", err, target.values["key"])
			}
		})
	}
}

func TestLiveIntentCommitFailureNeverInvokesSource(t *testing.T) {
	c, mock, _, _ := newLiveTest(t)
	expectLiveActiveGate(mock, liveTestRoute())
	expectLiveClean(mock)
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO data_migration_live_intents").WillReturnRows(sqlmock.NewRows([]string{"record_key"}).AddRow("key"))
	want := errors.New("intent commit lost")
	mock.ExpectCommit().WillReturnError(want)
	expectLiveUnlock(mock, false)
	err := c.WithWrite(context.Background(), "tenant-a", DomainSession, "source", []string{"key"},
		func(context.Context, string) error { t.Fatal("source ran before durable intent"); return nil })
	if !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
}

func TestLiveReadReconcilesCrashIntentBeforeTargetRead(t *testing.T) {
	c, mock, source, target := newLiveTest(t)
	route := liveTestRoute()
	route.ReadProfile = "target"
	expectLiveActiveGate(mock, route)
	mock.ExpectQuery("SELECT record_key FROM data_migration_live_intents").WillReturnRows(sqlmock.NewRows([]string{"record_key"}).AddRow("key"))
	source.values["key"] = []byte("source committed before crash")
	record, _ := source.Read(context.Background(), "key", 1)
	expectLiveCapture(mock, record)
	expectLivePending(mock, record)
	expectLiveProject(mock, record, false)
	expectLivePending(mock)
	mock.ExpectExec("DELETE FROM data_migration_live_intents").WithArgs("migration", liveKeyHash("key"), "key").WillReturnResult(sqlmock.NewResult(0, 1))
	expectLiveClean(mock)
	expectLiveUnlock(mock, false)
	err := c.WithRead(context.Background(), "tenant-a", DomainSession, "stale-source", func(_ context.Context, profile string) error {
		if profile != "target" || string(target.values["key"]) != "source committed before crash" {
			t.Fatalf("read escaped recovery: profile=%s target=%q", profile, target.values["key"])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLiveJournalRetainsDeleteBeforeRecreation(t *testing.T) {
	c, mock, source, target := newLiveTest(t)
	conn, err := c.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	access := &liveAccess{gate: &liveGate{conn: conn}, route: liveTestRoute(), source: source, target: target}
	target.values["key"] = []byte("old history")
	source.values["key"] = []byte("new history")
	deleted := LiveTombstone("key", 1)
	recreated, _ := source.Read(context.Background(), "key", 2)
	expectLivePending(mock, deleted, recreated)
	expectLiveProject(mock, deleted, false)
	expectLiveProject(mock, recreated, false)
	expectLivePending(mock)
	if err := c.projectPending(context.Background(), access); err != nil {
		t.Fatal(err)
	}
	if len(target.applied) != 2 || !target.applied[0].Deleted || target.applied[1].Deleted || string(target.values["key"]) != "new history" {
		t.Fatalf("lost deletion boundary: applied=%v values=%v", target.applied, target.values)
	}
}

func TestLiveTargetMismatchDoesNotAcknowledgeProjection(t *testing.T) {
	c, mock, source, target := newLiveTest(t)
	conn, _ := c.db.Conn(context.Background())
	defer conn.Close()
	access := &liveAccess{gate: &liveGate{conn: conn}, route: liveTestRoute(), source: source, target: target}
	source.values["key"] = []byte("expected")
	record, _ := source.Read(context.Background(), "key", 1)
	target.corrupt = true
	expectLiveProject(mock, record, true)
	if err := c.project(context.Background(), access, record); !errors.Is(err, ErrLiveVerification) {
		t.Fatalf("got %v", err)
	}
}

func TestLiveExpiredLeasePreventsTargetMutation(t *testing.T) {
	c, mock, source, target := newLiveTest(t)
	conn, _ := c.db.Conn(context.Background())
	defer conn.Close()
	access := &liveAccess{gate: &liveGate{conn: conn}, route: liveTestRoute(), source: source, target: target}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (SELECT 1 FROM data_migrations")).
		WithArgs("migration", "tenant-a", DomainSession, "owner", int64(1)).WillReturnRows(sqlmock.NewRows([]string{"current"}).AddRow(false))
	ctx := withLeaseFence(context.Background(), Job{ID: "migration", LeaseOwner: "owner", LeaseVersion: 1})
	if err := c.project(ctx, access, LiveTombstone("key", 1)); !errors.Is(err, ErrMigrationFence) || len(target.applied) != 0 {
		t.Fatalf("error=%v applied=%v", err, target.applied)
	}
}

func TestLivePeerMetadataRejectsAliasesAndConfigurationLies(t *testing.T) {
	_, _, source, target := newLiveTest(t)
	route := liveTestRoute()
	if err := validateLivePeers(source, target, route); err != nil {
		t.Fatal(err)
	}
	target.info.Identity = source.info.Identity
	if err := validateLivePeers(source, target, route); !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("same physical backend accepted: %v", err)
	}
	target.info.Identity, target.info.Compatibility = "other", "different embedding space"
	if err := validateLivePeers(source, target, route); !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("incompatible representation accepted: %v", err)
	}
	target.info.Compatibility, route.SourceBackend = source.info.Compatibility, "postgres"
	if err := validateLivePeers(source, target, route); !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("incorrect source backend accepted: %v", err)
	}
}

func TestLivePersistedProfileIdentityRejectsCrossProcessDrift(t *testing.T) {
	c, mock, _, target := newLiveTest(t)
	route := liveTestRoute()
	route.Mirroring, route.ReadProfile, route.WriteProfile = false, "target", "target"
	target.info.Identity = "different-store-under-the-same-profile"
	expectLiveGate(mock, route, true)
	expectLiveUnlock(mock, true)
	err := c.WithWrite(context.Background(), "tenant-a", DomainSession, "source", []string{"key"},
		func(context.Context, string) error { t.Fatal("writer escaped pinned backend identity"); return nil })
	if !errors.Is(err, ErrMigrationCapability) {
		t.Fatalf("profile drift was accepted: %v", err)
	}
}

func liveJobRows(job Job) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "tenant_id", "domain", "source_profile", "target_profile", "phase", "paused", "cursor",
		"snapshot_watermark", "applied_watermark", "lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at"}).AddRow(
		job.ID, job.TenantID, job.Domain, job.SourceProfile, job.TargetProfile, job.Phase, job.Paused, job.Cursor,
		job.SnapshotWatermark, job.AppliedWatermark, job.LeaseOwner, job.LeaseVersion, nullTime(job.LeaseUntil), job.LastError, job.CreatedAt, job.UpdatedAt)
}

func TestLiveAbortAfterUnrelatedConfigRevisionPreservesSource(t *testing.T) {
	c, mock, source, target := newLiveTest(t)
	now := time.Now()
	job, _ := NewJob("migration", "tenant-a", DomainSession, "source", "target", now)
	job.Phase, job.LastError = PhaseCutover, "configuration CAS conflict"
	mock.ExpectBegin()
	mock.ExpectQuery("FROM data_migrations.*WHERE id=\\$1 FOR UPDATE").WithArgs(job.ID).WillReturnRows(liveJobRows(job))
	mock.ExpectQuery("SELECT clock_timestamp").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(now))
	mock.ExpectExec("UPDATE data_migrations.*SET lease_owner").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	job.LeaseOwner, job.LeaseVersion, job.LeaseUntil = "owner", 1, now.Add(time.Minute)
	expectLiveGate(mock, liveTestRoute(), false)
	mock.ExpectBegin()
	// Route was created at revision 1; an unrelated edit advanced it to 2.
	mock.ExpectQuery("SELECT config_version,COALESCE").WithArgs("tenant-a", "sessionBackend", "sessionProfile").
		WillReturnRows(sqlmock.NewRows([]string{"config_version", "backend", "profile"}).AddRow(2, "redis", "source"))
	mock.ExpectExec("UPDATE data_migration_live_routes SET mirroring=FALSE").WithArgs("migration", "source", int64(2)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO control_plane_audit").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE data_migrations SET phase='ROLLED_BACK'").WithArgs("migration", "owner", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectLiveUnlock(mock, false)
	job.Phase, job.LastError = PhaseRolledBack, ""
	mock.ExpectQuery("FROM data_migrations.*WHERE id=\\$1").WithArgs(job.ID).WillReturnRows(liveJobRows(job))
	mock.ExpectBegin()
	mock.ExpectQuery("FROM data_migrations.*WHERE id=\\$1 FOR UPDATE").WithArgs(job.ID).WillReturnRows(liveJobRows(job))
	mock.ExpectQuery("SELECT clock_timestamp").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(now))
	mock.ExpectExec("UPDATE data_migrations.*SET lease_owner=NULL").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	actual, err := c.Abort(context.Background(), "migration", "operator", "retry after configuration change")
	if err != nil || actual.Phase != PhaseRolledBack || len(source.applied) != 0 || len(target.applied) != 0 {
		t.Fatalf("abort=%+v err=%v", actual, err)
	}
}

func TestLiveRollbackDoesNotResolveUnavailableTarget(t *testing.T) {
	c, mock, source, _ := newLiveTest(t)
	targetResolves := 0
	c.resolve = func(_ context.Context, _ string, _ Domain, profile string) (LiveBackend, func(), error) {
		if profile == "target" {
			targetResolves++
			return nil, nil, errors.New("target database is offline")
		}
		return source, func() {}, nil
	}
	now := time.Now()
	job, _ := NewJob("migration", "tenant-a", DomainSession, "source", "target", now)
	job.Phase, job.LeaseOwner, job.LeaseVersion, job.LeaseUntil = PhaseRollbackWindow, "owner", 1, now.Add(time.Minute)
	route := liveTestRoute()
	route.ReadProfile, route.ConfigVersion = "target", 2
	mock.ExpectQuery("FROM data_migrations.*WHERE id=\\$1").WithArgs(job.ID).WillReturnRows(liveJobRows(job))
	expectLiveGate(mock, route, false)
	expectFence := func() {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (SELECT 1 FROM data_migrations")).
			WithArgs("migration", "tenant-a", DomainSession, "owner", int64(1)).WillReturnRows(sqlmock.NewRows([]string{"current"}).AddRow(true))
	}
	expectFence()
	mock.ExpectQuery("SELECT record_key FROM data_migration_live_intents").WithArgs("migration", 128).
		WillReturnRows(sqlmock.NewRows([]string{"record_key"}))
	expectFence()
	mock.ExpectBegin()
	mock.ExpectQuery("FROM data_migrations.*WHERE id=\\$1 FOR UPDATE").WithArgs(job.ID).WillReturnRows(liveJobRows(job))
	mock.ExpectQuery("SELECT clock_timestamp").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(now))
	// Rollback preserves unrelated tenant edits at revision 9 and CASes that
	// revision only after confirming this domain still points at the target.
	mock.ExpectQuery("SELECT config_version,COALESCE").WithArgs("tenant-a", "sessionBackend", "sessionProfile").
		WillReturnRows(sqlmock.NewRows([]string{"config_version", "backend", "profile"}).AddRow(9, "postgres", "target"))
	mock.ExpectQuery("UPDATE tenants SET config=jsonb_set").
		WithArgs("tenant-a", "sessionBackend", "sessionProfile", "redis", "source", int64(9), "postgres", "target").
		WillReturnRows(sqlmock.NewRows([]string{"config_version"}).AddRow(10))
	mock.ExpectExec("UPDATE data_migration_live_routes SET read_profile").
		WithArgs("migration", "source", "source", false, int64(10)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO control_plane_audit").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE data_migrations SET phase=\\$4").WithArgs("migration", "owner", int64(1), PhaseRolledBack).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectLiveUnlock(mock, false)
	ctx := withLeaseFence(context.Background(), job)
	if err := c.switchRoute(ctx, job.ID, PhaseRolledBack, "operator", "target is unavailable"); err != nil {
		t.Fatal(err)
	}
	if targetResolves != 0 {
		t.Fatalf("rollback contacted target %d times", targetResolves)
	}
}

func TestLiveSupersededPendingReceiptIsAcknowledgedWithoutOverwrite(t *testing.T) {
	c, mock, _, target := newLiveTest(t)
	conn, err := c.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	access := &liveAccess{gate: &liveGate{conn: conn}, route: liveTestRoute(), target: target}
	record := LiveTombstone("key", 1)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (SELECT 1 FROM data_migration_live_journal")).
		WithArgs("migration", liveKeyHash("key"), int64(1)).WillReturnRows(sqlmock.NewRows([]string{"projected"}).AddRow(true))
	mock.ExpectExec("UPDATE data_migration_live_journal.*SET projected_at=COALESCE").
		WithArgs("migration", int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := c.project(context.Background(), access, record); err != nil {
		t.Fatal(err)
	}
	if len(target.applied) != 0 {
		t.Fatal("old tombstone overwrote newer committed target")
	}
}

var _ LiveBackend = (*liveTestBackend)(nil)
var _ LiveBackendMetadata = (*liveTestBackend)(nil)
