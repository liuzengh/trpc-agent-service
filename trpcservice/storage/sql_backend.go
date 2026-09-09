package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memorymysql "trpc.group/trpc-go/trpc-agent-go/memory/mysql"
	memorypostgres "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionmysql "trpc.group/trpc-go/trpc-agent-go/session/mysql"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	mysqlstorage "trpc.group/trpc-go/trpc-agent-go/storage/mysql"
	postgresstorage "trpc.group/trpc-go/trpc-agent-go/storage/postgres"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const sqlSessionEventLimit = 1000

var disabledMemoryTools = []string{
	frameworkmemory.AddToolName, frameworkmemory.UpdateToolName, frameworkmemory.DeleteToolName,
	frameworkmemory.ClearToolName, frameworkmemory.SearchToolName, frameworkmemory.LoadToolName,
}

// SQLBackend owns independent health, official Session, official Memory and
// platform Committer connections for one immutable StorageProfile.
type SQLBackend struct {
	profile     tenant.StorageProfile
	credential  string
	fingerprint persistence.BackendFingerprint
	limits      sessionfence.Limits

	mu          sync.Mutex
	closed      bool
	initialized bool
	generation  uint64
	sessions    sessionfence.StagingSession
	memories    frameworkmemory.Service
	committer   persistence.Committer
	healthCheck func(context.Context) error
	healthClose func() error
	commitClose func() error
	closeOnce   sync.Once
	closeErr    error
}

func NewSQLBackend(profile tenant.StorageProfile, credential string, limits sessionfence.Limits) (*SQLBackend, error) {
	normalized, err := tenant.NormalizeStorageProfile(profile)
	if err != nil || !normalized.Kind.IsSQL() {
		return nil, errors.New("invalid SQL storage profile")
	}
	fingerprint, err := persistence.FingerprintForProfile(normalized, credential)
	if err != nil {
		return nil, err
	}
	return &SQLBackend{profile: normalized, credential: credential, fingerprint: fingerprint, limits: limits}, nil
}

func (b *SQLBackend) Fingerprint() persistence.BackendFingerprint { return b.fingerprint }

func (b *SQLBackend) String() string {
	return fmt.Sprintf("SQLBackend{kind:%s tenant:%q profile:%q namespace:%q}", b.profile.Kind, b.profile.TenantID, b.profile.ID, b.fingerprint.Namespace)
}

func (b *SQLBackend) GoString() string { return b.String() }

func (b *SQLBackend) Ready(ctx context.Context) error {
	ctx, span := telemetry.Start(ctx, "storage.ready")
	defer span.End()
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return persistence.ErrBackendUnavailable
	}
	if b.initialized {
		if err := b.healthCheck(ctx); err != nil {
			telemetry.RecordStorageError(ctx, string(b.profile.Kind), "sql_unavailable")
			// A live SQL resource can become unusable after a database restart.
			// Release all stale clients before rebuilding them on the next Ready
			// call; this keeps recovery in-process and does not alter persistence
			// ordering or schema validation.
			_ = b.closeResourcesLocked()
			return persistence.ErrBackendUnavailable
		}
		return nil
	}
	if err := b.initializeLocked(ctx); err != nil {
		_ = b.closeResourcesLocked()
		return err
	}
	b.initialized = true
	b.generation++
	return nil
}

// ResourceGeneration changes whenever Session and Memory clients are rebuilt.
// Cached runners use it to stop borrowing services closed after a SQL restart.
func (b *SQLBackend) ResourceGeneration() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.generation
}

func (b *SQLBackend) initializeLocked(ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = persistence.ErrSchemaIncompatible
		}
	}()
	if b.profile.Kind == tenant.StorageKindPostgres {
		return b.initializePostgresLocked(ctx)
	}
	return b.initializeMySQLLocked(ctx)
}

func (b *SQLBackend) initializePostgresLocked(ctx context.Context) error {
	builder := postgresstorage.GetClientBuilder()
	health, err := builder(ctx, postgresstorage.WithClientConnString(b.credential))
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	b.healthClose = health.Close
	b.healthCheck = func(ctx context.Context) error {
		return health.Query(ctx, func(*sql.Rows) error { return nil }, "SELECT 1")
	}
	baseSession, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(b.credential), sessionpostgres.WithSchema(b.profile.Schema),
		sessionpostgres.WithTablePrefix(b.profile.TablePrefix), sessionpostgres.WithSkipDBInit(b.profile.SkipDBInit),
		sessionpostgres.WithEnableAsyncPersist(false), sessionpostgres.WithSessionTTL(0),
		sessionpostgres.WithSessionEventLimit(sqlSessionEventLimit),
	)
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	b.sessions = newSQLStagingSession(baseSession, b.limits, true)
	memoryOptions := []memorypostgres.ServiceOpt{
		memorypostgres.WithPostgresClientDSN(b.credential), memorypostgres.WithSchema(b.profile.Schema),
		memorypostgres.WithTableName(b.profile.TablePrefix + "memories"), memorypostgres.WithSkipDBInit(b.profile.SkipDBInit),
		memorypostgres.WithMemoryLimit(0),
	}
	for _, name := range disabledMemoryTools {
		memoryOptions = append(memoryOptions, memorypostgres.WithToolEnabled(name, false))
	}
	b.memories, err = memorypostgres.NewService(memoryOptions...)
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	commitClient, err := builder(ctx, postgresstorage.WithClientConnString(b.credential))
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	b.commitClose = commitClient.Close
	committer := newPostgresCommitter(commitClient, b.profile.Schema, b.profile.TablePrefix, b.profile.SkipDBInit)
	if err := committer.Ready(ctx); err != nil {
		return err
	}
	b.committer = committer
	return nil
}

func (b *SQLBackend) initializeMySQLLocked(ctx context.Context) error {
	builder := mysqlstorage.GetClientBuilder()
	health, err := builder(mysqlstorage.WithClientBuilderDSN(b.credential))
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	b.healthClose = health.Close
	b.healthCheck = func(ctx context.Context) error {
		var one int
		return health.QueryRow(ctx, []any{&one}, "SELECT 1")
	}
	baseSession, err := sessionmysql.NewService(
		sessionmysql.WithMySQLClientDSN(b.credential), sessionmysql.WithTablePrefix(b.profile.TablePrefix),
		sessionmysql.WithSkipDBInit(b.profile.SkipDBInit), sessionmysql.WithEnableAsyncPersist(false),
		sessionmysql.WithSessionTTL(0), sessionmysql.WithSessionEventLimit(sqlSessionEventLimit),
	)
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	b.sessions = newSQLStagingSession(baseSession, b.limits, false)
	memoryOptions := []memorymysql.ServiceOpt{
		memorymysql.WithMySQLClientDSN(b.credential), memorymysql.WithTableName(b.profile.TablePrefix + "memories"),
		memorymysql.WithSkipDBInit(b.profile.SkipDBInit), memorymysql.WithMemoryLimit(0),
	}
	for _, name := range disabledMemoryTools {
		memoryOptions = append(memoryOptions, memorymysql.WithToolEnabled(name, false))
	}
	b.memories, err = memorymysql.NewService(memoryOptions...)
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	commitClient, err := builder(mysqlstorage.WithClientBuilderDSN(b.credential))
	if err != nil {
		return persistence.ErrBackendUnavailable
	}
	b.commitClose = commitClient.Close
	committer := newMySQLCommitter(commitClient, b.profile.TablePrefix, b.profile.SkipDBInit)
	if err := committer.Ready(ctx); err != nil {
		return err
	}
	b.committer = committer
	return nil
}

func (b *SQLBackend) Check(ctx context.Context) error { return b.Ready(ctx) }

func (b *SQLBackend) Session() session.Service {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sessions
}
func (b *SQLBackend) Memory() frameworkmemory.Service {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.memories
}
func (b *SQLBackend) Committer() persistence.Committer {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committer
}

func (b *SQLBackend) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.closeErr = b.closeResourcesLocked()
		b.mu.Unlock()
	})
	return b.closeErr
}

func (b *SQLBackend) closeResourcesLocked() error {
	var errs []error
	if b.sessions != nil {
		errs = append(errs, b.sessions.Close())
	}
	if b.memories != nil {
		errs = append(errs, b.memories.Close())
	}
	if b.commitClose != nil {
		errs = append(errs, b.commitClose())
	}
	if b.healthClose != nil {
		errs = append(errs, b.healthClose())
	}
	b.sessions, b.memories, b.committer = nil, nil, nil
	b.healthCheck, b.healthClose, b.commitClose = nil, nil, nil
	b.initialized = false
	return errors.Join(errs...)
}

var _ Backend = (*SQLBackend)(nil)
