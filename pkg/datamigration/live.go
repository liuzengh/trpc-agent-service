package datamigration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// LiveBackend operates on a complete, canonical domain record. Read assigns
// the supplied coordinator version; an absent key is a valid tombstone.
// Keys must enumerate existing source keys completely. Apply must be
// idempotent, wait for its side effect to settle, and reject divergent data
// when the backend cannot safely reconstruct the requested state.
type LiveBackend interface {
	Keys(context.Context, string, int) ([]string, string, bool, error)
	Read(context.Context, string, int64) (Record, error)
	Apply(context.Context, Record) error
}

type LiveBackendInfo struct {
	Backend       string
	Identity      string
	Compatibility string
}

// LiveBackendMetadata identifies actual storage, not its profile alias.
// Identity must exclude secrets and distinguish endpoints and namespaces.
type LiveBackendMetadata interface {
	MigrationInfo() LiveBackendInfo
}

// A source may have committed a tombstone before its external cleanup failed.
// Reconciliation completes that confirmed side effect before capture; target
// reads remain strict and never repair the data they are meant to verify.
type LiveBackendSourceReconciler interface {
	ReconcileSource(context.Context, string) error
}

// LiveBackendTargetAdmission checks the physical target namespace when its
// inventory is shared metadata rather than a list of target objects.
type LiveBackendTargetAdmission interface {
	TargetEmpty(context.Context) (bool, error)
}

type LiveBackendResolver func(context.Context, string, Domain, string) (LiveBackend, func(), error)

type LiveOptions struct {
	DB *sql.DB
	// GateDB is a distinct, bounded pool connected to the same PostgreSQL
	// authority. A gate holds its connection while Resolve may query DB.
	GateDB    *sql.DB
	Resolve   LiveBackendResolver
	Owner     string
	LeaseTTL  time.Duration
	BatchSize int
}

type LiveRequest struct {
	ID                    string
	TenantID              string
	Domain                Domain
	SourceProfile         string
	TargetProfile         string
	SourceBackend         string
	TargetBackend         string
	ExpectedConfigVersion int64
	Actor                 string
	Reason                string
}

var (
	ErrLiveVerification = errors.New("online migration target does not match its source")
	ErrLiveDirty        = errors.New("online migration has unresolved source writes")
)

type LiveCoordinator struct {
	db        *sql.DB
	gateDB    *sql.DB
	resolve   LiveBackendResolver
	owner     string
	leaseTTL  time.Duration
	batchSize int
}

func NewLiveCoordinator(options LiveOptions) (*LiveCoordinator, error) {
	if options.LeaseTTL == 0 {
		options.LeaseTTL = 30 * time.Second
	}
	if options.BatchSize == 0 {
		options.BatchSize = 128
	}
	if options.DB == nil || options.GateDB == nil || options.GateDB == options.DB ||
		options.GateDB.Stats().MaxOpenConnections < 1 || options.Resolve == nil || options.BatchSize < 1 || options.BatchSize > 1000 {
		return nil, ErrMigrationCapability
	}
	if err := validateMigrationLease(options.Owner, options.LeaseTTL); err != nil {
		return nil, err
	}
	if max := options.DB.Stats().MaxOpenConnections; max > 0 && max < 3 {
		return nil, fmt.Errorf("%w: online migration requires at least three database connections", ErrMigrationCapability)
	}
	return &LiveCoordinator{db: options.DB, gateDB: options.GateDB, resolve: options.Resolve, owner: options.Owner,
		leaseTTL: options.LeaseTTL, batchSize: options.BatchSize}, nil
}

type liveRoute struct {
	JobID, TenantID, SourceProfile, TargetProfile string
	Domain                                        Domain
	SourceBackend, TargetBackend                  string
	SourceIdentity, TargetIdentity                string
	Compatibility                                 string
	ReadProfile, WriteProfile                     string
	Mirroring                                     bool
	ConfigVersion                                 int64
	ScanCursor                                    string
	ScanDone                                      bool
	Actor, Reason                                 string
}

const liveRouteSelect = `SELECT r.migration_id,r.tenant_id,r.domain,m.source_profile,m.target_profile,
	r.source_backend,r.target_backend,r.read_profile,r.write_profile,r.mirroring,
	r.config_version,r.scan_cursor,r.scan_done,r.actor,r.reason,
	r.source_identity,r.target_identity,r.compatibility
	FROM data_migration_live_routes r JOIN data_migrations m ON m.id=r.migration_id`

func scanLiveRoute(row rowScanner) (liveRoute, error) {
	var route liveRoute
	err := row.Scan(&route.JobID, &route.TenantID, &route.Domain, &route.SourceProfile, &route.TargetProfile,
		&route.SourceBackend, &route.TargetBackend, &route.ReadProfile, &route.WriteProfile, &route.Mirroring,
		&route.ConfigVersion, &route.ScanCursor, &route.ScanDone, &route.Actor, &route.Reason,
		&route.SourceIdentity, &route.TargetIdentity, &route.Compatibility)
	return route, err
}

func liveScope(tenantID string, domain Domain) (string, error) {
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return "", ErrInvalidMigration
	}
	if domain != DomainSession && domain != DomainKnowledge && domain != DomainArtifact {
		return "", ErrMigrationCapability
	}
	return "online-data-migration/v1/" + tenantID + "/" + string(domain), nil
}

// A dedicated connection retains the session advisory gate across the intent
// commit and the external operation. Close always unlocks before pooling it.
type liveGate struct {
	conn   *sql.Conn
	scope  string
	shared bool
}

func (c *LiveCoordinator) gate(ctx context.Context, tenantID string, domain Domain, shared bool) (*liveGate, error) {
	if c == nil || c.db == nil || c.gateDB == nil {
		return nil, ErrMigrationCapability
	}
	scope, err := liveScope(tenantID, domain)
	if err != nil {
		return nil, err
	}
	conn, err := c.gateDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	query := `SELECT pg_advisory_lock(hashtextextended($1, 0))`
	if shared {
		query = `SELECT pg_advisory_lock_shared(hashtextextended($1, 0))`
	}
	if _, err := conn.ExecContext(ctx, query, scope); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
		return nil, err
	}
	return &liveGate{conn: conn, scope: scope, shared: shared}, nil
}

func (g *liveGate) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), migrationPersistenceTimeout)
	defer cancel()
	query := `SELECT pg_advisory_unlock(hashtextextended($1, 0))`
	if g.shared {
		query = `SELECT pg_advisory_unlock_shared(hashtextextended($1, 0))`
	}
	var unlocked bool
	err := g.conn.QueryRowContext(ctx, query, g.scope).Scan(&unlocked)
	if err != nil || !unlocked {
		// A connection with an uncertain session lock must never return to the
		// pool. ErrBadConn tells database/sql to discard it.
		_ = g.conn.Raw(func(any) error { return driver.ErrBadConn })
		if err == nil {
			err = ErrMigrationFence
		}
	}
	return errors.Join(err, g.conn.Close())
}

func (c *LiveCoordinator) route(ctx context.Context, conn *sql.Conn, tenantID string, domain Domain) (liveRoute, bool, error) {
	route, err := scanLiveRoute(conn.QueryRowContext(ctx, liveRouteSelect+` WHERE r.tenant_id=$1 AND r.domain=$2`, tenantID, domain))
	if errors.Is(err, sql.ErrNoRows) {
		return liveRoute{}, false, nil
	}
	return route, err == nil, err
}

func (c *LiveCoordinator) resolveBackend(ctx context.Context, route liveRoute, profile string) (LiveBackend, func(), error) {
	backend, release, err := c.resolve(ctx, route.TenantID, route.Domain, profile)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, nil, err
	}
	if isNilMigrationDependency(backend) || release == nil {
		if release != nil {
			release()
		}
		return nil, nil, ErrMigrationCapability
	}
	return backend, release, nil
}

func validateLiveActor(actor, reason string) error {
	if strings.TrimSpace(actor) == "" || len(actor) > 256 || !utf8.ValidString(actor) || strings.ContainsAny(actor, "\x00\r\n\t") ||
		strings.TrimSpace(reason) == "" || len(reason) > 2048 || !utf8.ValidString(reason) || strings.ContainsAny(reason, "\x00\r\n\t") {
		return ErrInvalidMigration
	}
	return nil
}

func liveStorageKeys(domain Domain) (string, string, error) {
	switch domain {
	case DomainSession:
		return "sessionBackend", "sessionProfile", nil
	case DomainKnowledge:
		return "knowledgeBackend", "knowledgeProfile", nil
	case DomainArtifact:
		return "artifactBackend", "artifactProfile", nil
	default:
		return "", "", ErrMigrationCapability
	}
}

func liveAudit(ctx context.Context, tx *sql.Tx, route liveRoute, actor, reason, action string) error {
	data, err := json.Marshal(map[string]any{"domain": route.Domain, "sourceProfile": route.SourceProfile,
		"targetProfile": route.TargetProfile, "reason": sanitizeMigrationErrorText(reason)})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO control_plane_audit
		(tenant_id,actor,action,resource_type,resource_id,details)
		VALUES ($1,$2,$3,'data_migration',$4,$5::jsonb)`, route.TenantID, actor, action, route.JobID, string(data))
	return err
}

func (c *LiveCoordinator) Create(ctx context.Context, request LiveRequest) (job Job, resultErr error) {
	ctx = nonNilMigrationContext(ctx)
	job, err := NewJob(request.ID, request.TenantID, request.Domain, request.SourceProfile, request.TargetProfile, time.Now())
	if err != nil {
		return Job{}, err
	}
	if err := validateLiveActor(request.Actor, request.Reason); err != nil || request.ExpectedConfigVersion <= 0 {
		return Job{}, ErrInvalidMigration
	}
	gate, err := c.gate(ctx, request.TenantID, request.Domain, false)
	if err != nil {
		return Job{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, gate.close()) }()
	previous, exists, err := c.route(ctx, gate.conn, request.TenantID, request.Domain)
	if err != nil {
		return Job{}, err
	}
	if exists && previous.Mirroring {
		return Job{}, ErrMigrationConflict
	}
	route := liveRoute{JobID: job.ID, TenantID: job.TenantID, Domain: job.Domain,
		SourceProfile: job.SourceProfile, TargetProfile: job.TargetProfile,
		SourceBackend: request.SourceBackend, TargetBackend: request.TargetBackend,
		ReadProfile: job.SourceProfile, WriteProfile: job.SourceProfile, Mirroring: true,
		ConfigVersion: request.ExpectedConfigVersion, Actor: request.Actor, Reason: request.Reason}
	source, releaseSource, err := c.resolveBackend(ctx, route, route.SourceProfile)
	if err != nil {
		return Job{}, err
	}
	defer releaseSource()
	target, releaseTarget, err := c.resolveBackend(ctx, route, route.TargetProfile)
	if err != nil {
		return Job{}, err
	}
	defer releaseTarget()
	if err := validateLivePeers(source, target, route); err != nil {
		return Job{}, err
	}
	sourceInfo := source.(LiveBackendMetadata).MigrationInfo()
	targetInfo := target.(LiveBackendMetadata).MigrationInfo()
	route.SourceIdentity, route.TargetIdentity, route.Compatibility = sourceInfo.Identity, targetInfo.Identity, sourceInfo.Compatibility
	if _, _, _, err := source.Keys(ctx, "", 1); err != nil {
		return Job{}, err
	}
	empty := false
	if admission, ok := target.(LiveBackendTargetAdmission); ok {
		empty, err = admission.TargetEmpty(ctx)
	} else {
		var keys []string
		var done bool
		keys, _, done, err = target.Keys(ctx, "", 1)
		empty = len(keys) == 0 && done
	}
	if err != nil {
		return Job{}, err
	}
	if !empty {
		return Job{}, fmt.Errorf("%w: target namespace must be empty", ErrMigrationConflict)
	}
	tx, err := gate.conn.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()
	backendKey, profileKey, _ := liveStorageKeys(job.Domain)
	var version int64
	var backend, profile string
	if err := tx.QueryRowContext(ctx, `SELECT config_version,COALESCE(config->'storage'->>$2,''),
		COALESCE(config->'storage'->>$3,'') FROM tenants WHERE id=$1 AND status='active' FOR UPDATE`,
		job.TenantID, backendKey, profileKey).Scan(&version, &backend, &profile); err != nil {
		return Job{}, err
	}
	if version != route.ConfigVersion || backend != route.SourceBackend || profile != route.SourceProfile ||
		(exists && previous.WriteProfile != route.SourceProfile) {
		return Job{}, ErrMigrationConflict
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO data_migrations
		(id,tenant_id,domain,source_profile,target_profile,phase)
		VALUES ($1,$2,$3,$4,$5,'PREPARE')`, job.ID, job.TenantID, job.Domain, job.SourceProfile, job.TargetProfile)
	if err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Job{}, ErrMigrationConflict
		}
		return Job{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO data_migration_live_routes
		(tenant_id,domain,migration_id,source_backend,target_backend,read_profile,write_profile,config_version,actor,reason,
		source_identity,target_identity,compatibility)
		VALUES ($1,$2,$3,$4,$5,$6,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (tenant_id,domain) DO UPDATE SET migration_id=EXCLUDED.migration_id,
		source_backend=EXCLUDED.source_backend,target_backend=EXCLUDED.target_backend,
		read_profile=EXCLUDED.read_profile,write_profile=EXCLUDED.write_profile,
		config_version=EXCLUDED.config_version,actor=EXCLUDED.actor,reason=EXCLUDED.reason,
		source_identity=EXCLUDED.source_identity,target_identity=EXCLUDED.target_identity,compatibility=EXCLUDED.compatibility,
		mirroring=TRUE,scan_cursor='',scan_done=FALSE,updated_at=clock_timestamp()`,
		job.TenantID, job.Domain, job.ID, route.SourceBackend, route.TargetBackend, route.SourceProfile,
		route.ConfigVersion, route.Actor, route.Reason, route.SourceIdentity, route.TargetIdentity, route.Compatibility)
	if err != nil {
		return Job{}, err
	}
	if err := liveAudit(ctx, tx, route, request.Actor, request.Reason, "data_migration.create"); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, err
	}
	return job, nil
}

func validateLivePeers(source, target LiveBackend, route liveRoute) error {
	sourceMetadata, sourceOK := source.(LiveBackendMetadata)
	targetMetadata, targetOK := target.(LiveBackendMetadata)
	if !sourceOK || !targetOK {
		return ErrMigrationCapability
	}
	s, t := sourceMetadata.MigrationInfo(), targetMetadata.MigrationInfo()
	if s.Backend != route.SourceBackend || t.Backend != route.TargetBackend ||
		!validLiveInfo(s) || !validLiveInfo(t) || s.Identity == t.Identity || s.Compatibility != t.Compatibility ||
		(route.SourceIdentity != "" && route.SourceIdentity != s.Identity) ||
		(route.TargetIdentity != "" && route.TargetIdentity != t.Identity) ||
		(route.Compatibility != "" && route.Compatibility != s.Compatibility) {
		return fmt.Errorf("%w: backend identity or migration format mismatch", ErrMigrationCapability)
	}
	return nil
}

func validLiveInfo(info LiveBackendInfo) bool {
	return info.Identity != "" && len(info.Identity) <= 2048 && info.Compatibility != "" && len(info.Compatibility) <= 4096 &&
		utf8.ValidString(info.Identity) && utf8.ValidString(info.Compatibility) &&
		!strings.ContainsAny(info.Identity+info.Compatibility, "\x00\r\n\t")
}

func validateLiveProfile(backend LiveBackend, route liveRoute, profile string) error {
	metadata, ok := backend.(LiveBackendMetadata)
	if !ok {
		return ErrMigrationCapability
	}
	info := metadata.MigrationInfo()
	wantBackend, wantIdentity := route.SourceBackend, route.SourceIdentity
	if profile == route.TargetProfile {
		wantBackend, wantIdentity = route.TargetBackend, route.TargetIdentity
	} else if profile != route.SourceProfile {
		return ErrMigrationConflict
	}
	if !validLiveInfo(info) || wantIdentity == "" || info.Backend != wantBackend || info.Identity != wantIdentity || info.Compatibility != route.Compatibility {
		return fmt.Errorf("%w: persisted migration profile identity changed", ErrMigrationCapability)
	}
	return nil
}

func (c *LiveCoordinator) checkSelectedProfile(ctx context.Context, route liveRoute, profile string) error {
	backend, release, err := c.resolveBackend(ctx, route, profile)
	if err != nil {
		return err
	}
	defer release()
	return validateLiveProfile(backend, route, profile)
}

func (c *LiveCoordinator) Get(ctx context.Context, id string) (Job, error) {
	if c == nil || c.db == nil {
		return Job{}, ErrMigrationCapability
	}
	return NewPostgresStore(c.db).Get(ctx, id)
}

func liveKeyHash(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])
}
