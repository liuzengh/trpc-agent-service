package migration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestExecutorCopiesAndVerifiesSessions(t *testing.T) {
	t.Parallel()
	record := migration.Record{
		ID:                  "migration-1",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusDraining,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	key := session.Key{AppName: "tenant:tenant-1:app:app-1:runner", UserID: "user-1", SessionID: "session-1"}
	repository := &testMigrationRepository{}
	copier := &testSessionCopier{}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{keys: []session.Key{key}},
		Repository: repository,
		Copier:     copier,
	}

	if err := executor.Run(context.Background(), record); err != nil {
		t.Fatalf("run migration: %v", err)
	}
	if copier.copied != 1 || copier.verified != 1 {
		t.Fatalf("copied=%d verified=%d, want 1 each", copier.copied, copier.verified)
	}
	if len(repository.transitions) != 3 ||
		repository.transitions[0] != migration.StatusCopying ||
		repository.transitions[1] != migration.StatusVerifying ||
		repository.transitions[2] != migration.StatusSucceeded {
		t.Fatalf("transitions=%v, want COPYING VERIFYING SUCCEEDED", repository.transitions)
	}
}

func TestExecutorCopiesAndVerifiesKnowledge(t *testing.T) {
	t.Parallel()
	record := migration.Record{
		ID:                  "knowledge-migration-1",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		Domain:              migration.DomainKnowledge,
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusDraining,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	ref := platformknowledge.ChunkRef{
		Scope:           tenant.Scope{TenantID: "tenant-1", AppID: "app-1"},
		ConfigVersion:   "v1",
		KnowledgeBaseID: "base-1",
		DocumentID:      "document-1",
		DocumentVersion: "1",
		ChunkID:         "chunk-1",
		IndexGeneration: "generation-1",
	}
	repository := &testMigrationRepository{}
	copier := &testKnowledgeCopier{}
	executor := migration.Executor{
		KnowledgeCatalog: testKnowledgeCatalog{refs: []platformknowledge.ChunkRef{ref}},
		Repository:       repository,
		KnowledgeCopier:  copier,
	}

	if err := executor.Run(context.Background(), record); err != nil {
		t.Fatalf("run knowledge migration: %v", err)
	}
	if copier.copied != 1 || copier.verified != 1 {
		t.Fatalf("copied=%d verified=%d, want 1 each", copier.copied, copier.verified)
	}
	if len(repository.transitions) != 3 ||
		repository.transitions[0] != migration.StatusCopying ||
		repository.transitions[1] != migration.StatusVerifying ||
		repository.transitions[2] != migration.StatusSucceeded {
		t.Fatalf("transitions=%v, want COPYING VERIFYING SUCCEEDED", repository.transitions)
	}
}

func TestExecutorFailsWhenDrainDeadlineExpires(t *testing.T) {
	t.Parallel()
	record := migration.Record{
		ID:                  "migration-1",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusDraining,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	repository := &testMigrationRepository{advanceErr: migration.ErrDrainDeadlineExceeded}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{},
		Repository: repository,
		Copier:     &testSessionCopier{},
	}

	err := executor.Run(context.Background(), record)
	if !errors.Is(err, migration.ErrDrainDeadlineExceeded) {
		t.Fatalf("run migration error = %v, want drain deadline exceeded", err)
	}
	if len(repository.transitions) != 2 ||
		repository.transitions[0] != migration.StatusCopying ||
		repository.transitions[1] != migration.StatusFailed {
		t.Fatalf("transitions=%v, want COPYING FAILED", repository.transitions)
	}
	if repository.failureReason == "" {
		t.Fatalf("failure reason is empty")
	}
}

func TestExecutorLeavesDurablePhaseOpenForRetryableBackendFailure(t *testing.T) {
	t.Parallel()
	record := migration.Record{
		ID:                  "migration-retry",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusCopying,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	repository := &testMigrationRepository{}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{keys: []session.Key{{AppName: "tenant:tenant-1:app:app-1:runner", UserID: "user-1", SessionID: "session-1"}}},
		Repository: repository,
		Copier:     &retryingSessionCopier{},
	}

	err := executor.Run(context.Background(), record)
	if !errors.Is(err, errMigrationBackendUnavailable) {
		t.Fatalf("run migration error = %v, want backend unavailable", err)
	}
	if len(repository.transitions) != 0 {
		t.Fatalf("durable transitions = %v, want no terminal transition", repository.transitions)
	}
}

func TestExecutorRetryConvergesAfterPartialCopy(t *testing.T) {
	record := migration.Record{
		ID:                  "migration-partial-copy",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusCopying,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	keys := []session.Key{
		{AppName: "app", UserID: "user", SessionID: "session-1"},
		{AppName: "app", UserID: "user", SessionID: "session-2"},
	}
	repository := &checkpointMigrationRepository{}
	copier := &retryAfterPartialSessionCopier{}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{keys: keys},
		Repository: repository,
		Copier:     copier,
	}

	if err := executor.Run(context.Background(), record); !errors.Is(err, errMigrationBackendUnavailable) {
		t.Fatalf("first run error = %v, want backend unavailable", err)
	}
	if len(repository.checkpoints) == 0 {
		t.Fatal("partial-copy checkpoint was not persisted")
	}
	resume := repository.checkpoints[len(repository.checkpoints)-1]
	if resume.Status != migration.StatusCopying || resume.CopyProgress != 1 {
		t.Fatalf("resume checkpoint = %+v, want COPYING with one copied session", resume)
	}
	if len(copier.copied) != 1 || copier.copied[0].SessionID != "session-1" {
		t.Fatalf("partial target copied sessions = %v, want session-1", copier.copied)
	}

	if err := executor.Run(context.Background(), resume); err != nil {
		t.Fatalf("retry run: %v", err)
	}
	if len(copier.copied) != 2 || copier.copied[1].SessionID != "session-2" {
		t.Fatalf("retry copied sessions = %v, want only session-2 appended", copier.copied)
	}
	if len(copier.verified) != len(keys) {
		t.Fatalf("verified sessions = %d, want %d", len(copier.verified), len(keys))
	}
	if got := repository.transitions[len(repository.transitions)-1]; got != migration.StatusSucceeded {
		t.Fatalf("final transition = %s, want SUCCEEDED", got)
	}
}

func TestExecutorResumesFromCheckpoint(t *testing.T) {
	record := migration.Record{
		ID:                  "migration-resume",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusCopying,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
		TotalSessions:       3,
		CopyProgress:        1,
		SuccessCount:        1,
	}
	keys := []session.Key{
		{AppName: "app", UserID: "user", SessionID: "session-1"},
		{AppName: "app", UserID: "user", SessionID: "session-2"},
		{AppName: "app", UserID: "user", SessionID: "session-3"},
	}
	repository := &checkpointMigrationRepository{}
	copier := &recordingSessionCopier{}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{keys: keys},
		Repository: repository,
		Copier:     copier,
	}

	if err := executor.Run(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(copier.copied) != 2 || copier.copied[0].SessionID != "session-2" || copier.copied[1].SessionID != "session-3" {
		t.Fatalf("copied sessions=%v, want session-2/session-3", copier.copied)
	}
	if len(copier.verified) != len(keys) {
		t.Fatalf("verified sessions=%d, want %d", len(copier.verified), len(keys))
	}
	if len(repository.checkpoints) == 0 || repository.checkpoints[len(repository.checkpoints)-1].CopyProgress != 3 {
		t.Fatalf("last checkpoint=%v, want copy progress 3", repository.checkpoints)
	}
}

type testSessionCatalog struct {
	keys []session.Key
	err  error
}

type testKnowledgeCatalog struct {
	refs []platformknowledge.ChunkRef
	err  error
}

func (c testKnowledgeCatalog) ListKnowledgeMigrationChunks(
	_ context.Context,
	_ migration.Record,
) ([]platformknowledge.ChunkRef, error) {
	return c.refs, c.err
}

func (c testSessionCatalog) ListDataMigrationSessionKeys(
	_ context.Context,
	_ migration.Record,
) ([]session.Key, error) {
	return c.keys, c.err
}

type testMigrationRepository struct {
	transitions   []migration.Status
	advanceErr    error
	failureReason string
}

type checkpointMigrationRepository struct {
	testMigrationRepository
	checkpoints []migration.Record
}

func (r *checkpointMigrationRepository) UpdateDataMigrationCheckpoint(_ context.Context, record migration.Record) error {
	r.checkpoints = append(r.checkpoints, record)
	return nil
}

type recordingSessionCopier struct {
	copied   []session.Key
	verified []session.Key
}

func (c *recordingSessionCopier) CopySession(_ context.Context, key session.Key) error {
	c.copied = append(c.copied, key)
	return nil
}

func (c *recordingSessionCopier) VerifySession(_ context.Context, key session.Key) error {
	c.verified = append(c.verified, key)
	return nil
}

func (r *testMigrationRepository) AdvanceDataMigration(
	_ context.Context,
	record migration.Record,
	next migration.Status,
) error {
	r.transitions = append(r.transitions, next)
	if next == migration.StatusCopying && r.advanceErr != nil {
		return r.advanceErr
	}
	if next == migration.StatusFailed {
		r.failureReason = record.FailureReason
	}
	return nil
}

type testSessionCopier struct {
	copied   int
	verified int
}

type testKnowledgeCopier struct {
	copied   int
	verified int
}

func (c *testKnowledgeCopier) CopyKnowledgeChunk(
	_ context.Context,
	_ platformknowledge.ChunkRef,
) error {
	c.copied++
	return nil
}

func (c *testKnowledgeCopier) VerifyKnowledgeChunk(
	_ context.Context,
	_ platformknowledge.ChunkRef,
) error {
	c.verified++
	return nil
}

var errMigrationBackendUnavailable = errors.New("migration backend unavailable")

type retryingSessionCopier struct{}

func (*retryingSessionCopier) CopySession(_ context.Context, _ session.Key) error {
	return migration.NewRetryableError(errMigrationBackendUnavailable)
}

func (*retryingSessionCopier) VerifySession(_ context.Context, _ session.Key) error { return nil }

type retryAfterPartialSessionCopier struct {
	copied   []session.Key
	verified []session.Key
	failed   bool
}

func (c *retryAfterPartialSessionCopier) CopySession(_ context.Context, key session.Key) error {
	if key.SessionID == "session-2" && !c.failed {
		c.failed = true
		return migration.NewRetryableError(errMigrationBackendUnavailable)
	}
	c.copied = append(c.copied, key)
	return nil
}

func (c *retryAfterPartialSessionCopier) VerifySession(_ context.Context, key session.Key) error {
	c.verified = append(c.verified, key)
	return nil
}

func (c *testSessionCopier) CopySession(_ context.Context, _ session.Key) error {
	c.copied++
	return nil
}

func (c *testSessionCopier) VerifySession(_ context.Context, _ session.Key) error {
	c.verified++
	return nil
}
