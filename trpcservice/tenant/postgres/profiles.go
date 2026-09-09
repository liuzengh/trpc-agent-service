package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const profileColumns = `tenant_id, id, spec, fingerprint, created_by, created_at`

const (
	insertProfileSQL = `INSERT INTO backend_profiles (` + profileColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6)`

	selectProfileSQL = `SELECT ` + profileColumns + `
		FROM backend_profiles WHERE tenant_id = $1 AND id = $2`

	// listProfilesSQL orders by the C collation, not the database's default.
	//
	// The order is part of the ProfileRepository contract, and the in-memory
	// implementation sorts Go strings — which is byte order. A database
	// initialised with a locale-aware default collation sorts "p-Alpha" and
	// "p-alpha" the other way round, so leaving the collation to the server
	// would make the admin API's output depend on how the database was created.
	listProfilesSQL = `SELECT ` + profileColumns + `
		FROM backend_profiles WHERE tenant_id = $1 ORDER BY id COLLATE "C"`

	countProfilesSQL = `SELECT count(*) FROM backend_profiles WHERE tenant_id = $1`

	// selectProfileTakenSQL asks whether one id is already used, and nothing
	// else: the row's contents are not part of the answer, and a create that
	// read them would be a create that could report on a profile the caller has
	// no other way to see.
	selectProfileTakenSQL = `SELECT 1 FROM backend_profiles
		WHERE tenant_id = $1 AND id = $2`

	// selectTenantForProfileWriteSQL locks the tenant row for the duration of a
	// profile create, which is what makes MaxProfilesPerTenant a real bound:
	// counting and inserting under one lock means two concurrent creates cannot
	// both see one slot free.
	//
	// FOR NO KEY UPDATE rather than FOR UPDATE. It conflicts with itself, which
	// is all that is needed to serialise profile creates, but it does not
	// conflict with the FOR KEY SHARE that every app and revision insert takes
	// on the same row to enforce its foreign key. With FOR UPDATE, creating a
	// profile would block every unrelated create in that tenant for as long as
	// the transaction ran.
	selectTenantForProfileWriteSQL = `SELECT status FROM tenants
		WHERE id = $1 FOR NO KEY UPDATE`
)

// ProfileRepository is the PostgreSQL implementation of
// storagebundle.ProfileRepository.
//
// It is the control plane's storage for backend profiles, and it is deliberately
// in this package rather than beside the Router: a profile belongs to a tenant,
// the tenant row is what gates writing one, and the two live in one database so
// that the foreign key can say so. It borrows the same pool the tenant
// Repository does, and its table is created by the same Migrate.
//
// Like Repository it never closes the pool, holds nothing that needs closing,
// and is safe for concurrent use.
type ProfileRepository struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

var _ storagebundle.ProfileRepository = (*ProfileRepository)(nil)

// NewProfileRepository wraps an existing pool. It does not connect or migrate;
// see the package documentation.
func NewProfileRepository(pool *pgxpool.Pool) (*ProfileRepository, error) {
	if pool == nil {
		return nil, errInvalidPool()
	}
	return &ProfileRepository{pool: pool, now: time.Now}, nil
}

// storedNow returns the timestamp a write should record, truncated for the
// reason Repository.storedNow documents.
func (r *ProfileRepository) storedNow() time.Time {
	return r.now().UTC().Truncate(time.Microsecond)
}

// CreateProfile implements storagebundle.ProfileRepository.
//
// Everything decidable from the request alone is decided before the database is
// touched, so a malformed profile never reaches it. The rest runs in one
// transaction that locks the tenant row first: the lock is what makes the count
// and the insert one decision, and taking it before the count is what stops two
// concurrent creates from both finding room.
func (r *ProfileRepository) CreateProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profile storagebundle.Profile,
	createdBy string,
) (storagebundle.ProfileRecord, error) {
	if err := contextError(ctx); err != nil {
		return storagebundle.ProfileRecord{}, err
	}
	if err := scope.Validate(); err != nil {
		return storagebundle.ProfileRecord{}, err
	}
	if profile.TenantID != scope.TenantID {
		return storagebundle.ProfileRecord{}, fmt.Errorf(
			"%w: backend profile belongs to another tenant", tenant.ErrTenantScope)
	}
	if createdBy == "" {
		return storagebundle.ProfileRecord{}, fmt.Errorf(
			"%w: backend profile requires a creator", tenant.ErrInvalidArgument)
	}
	// Fingerprint validates first, so this is also the shape check.
	fingerprint, err := profile.Fingerprint()
	if err != nil {
		return storagebundle.ProfileRecord{}, err
	}
	spec, err := encodeJSON(profile)
	if err != nil {
		return storagebundle.ProfileRecord{}, storageFailure("create backend profile", err)
	}

	record := storagebundle.ProfileRecord{
		Profile:     profile,
		Fingerprint: fingerprint,
		CreatedBy:   createdBy,
		CreatedAt:   r.storedNow(),
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return storagebundle.ProfileRecord{}, storageError(ctx, "begin create backend profile", err, nil)
	}
	// A no-op once the transaction has committed.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.lockActiveTenant(ctx, tx, scope.TenantID); err != nil {
		return storagebundle.ProfileRecord{}, err
	}

	// The id is checked before the count, so a full tenant still answers "that
	// id is taken" rather than "you are full". The two say different things —
	// one is about the request and one is about the tenant — and only the first
	// is something the caller can act on, since profiles cannot be deleted.
	//
	// This read decides which refusal is reported, not whether the id is unique:
	// the primary key below is still what enforces that. It is exact under
	// concurrency anyway, because the tenant row is locked above, which is what
	// stops another create from inserting this id between here and the insert.
	var taken int
	switch err := tx.QueryRow(
		ctx, selectProfileTakenSQL, scope.TenantID, record.ID,
	).Scan(&taken); {
	case err == nil:
		return storagebundle.ProfileRecord{}, fmt.Errorf(
			"%w: backend profile %q of tenant %q",
			tenant.ErrAlreadyExists, record.ID, record.TenantID)
	case !errors.Is(err, pgx.ErrNoRows):
		return storagebundle.ProfileRecord{}, storageError(
			ctx, "check backend profile id", err, nil)
	}

	var count int64
	if err := tx.QueryRow(ctx, countProfilesSQL, scope.TenantID).Scan(&count); err != nil {
		return storagebundle.ProfileRecord{}, storageError(ctx, "count backend profiles", err, nil)
	}
	if count >= storagebundle.MaxProfilesPerTenant {
		return storagebundle.ProfileRecord{}, fmt.Errorf(
			"%w: tenant %q may own %d",
			storagebundle.ErrProfileLimit, scope.TenantID, storagebundle.MaxProfilesPerTenant)
	}

	if _, err := tx.Exec(ctx, insertProfileSQL,
		record.TenantID, record.ID, spec, record.Fingerprint, record.CreatedBy, record.CreatedAt,
	); err != nil {
		return storagebundle.ProfileRecord{}, storageError(
			ctx, "create backend profile", err, map[string]error{
				constraintProfilesPrimaryKey: fmt.Errorf(
					"%w: backend profile %q of tenant %q",
					tenant.ErrAlreadyExists, record.ID, record.TenantID),
				// Unreachable while the tenant row is locked above, which
				// requires it to exist. It stays because the constraint is what
				// actually enforces the rule.
				constraintProfilesTenant: notFound("tenant", record.TenantID),
			})
	}
	if err := tx.Commit(ctx); err != nil {
		return storagebundle.ProfileRecord{}, storageError(
			ctx, "commit create backend profile", err, map[string]error{
				constraintProfilesPrimaryKey: fmt.Errorf(
					"%w: backend profile %q of tenant %q",
					tenant.ErrAlreadyExists, record.ID, record.TenantID),
			})
	}
	return record, nil
}

// GetProfile implements storagebundle.ProfileRepository.
func (r *ProfileRepository) GetProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (storagebundle.ProfileRecord, error) {
	return r.lookup(ctx, scope, profileID)
}

// ResolveProfile implements storagebundle.ProfileSource.
//
// This is the method Router calls on every resolution, which is why it hands
// back the Profile alone: the provenance is control-plane data, and the data
// plane has no use for it.
func (r *ProfileRepository) ResolveProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (storagebundle.Profile, error) {
	record, err := r.lookup(ctx, scope, profileID)
	if err != nil {
		return storagebundle.Profile{}, err
	}
	return record.Profile, nil
}

// ListProfiles implements storagebundle.ProfileRepository.
func (r *ProfileRepository) ListProfiles(
	ctx context.Context,
	scope tenant.TenantContext,
) ([]storagebundle.ProfileRecord, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, listProfilesSQL, scope.TenantID)
	if err != nil {
		return nil, storageError(ctx, "list backend profiles", err, nil)
	}
	defer rows.Close()

	records := make([]storagebundle.ProfileRecord, 0)
	for rows.Next() {
		record, tenantID, profileID, err := scanProfile(rows)
		if err != nil {
			return nil, storageError(ctx, "list backend profiles", err, nil)
		}
		// Every row before any is returned: a list that answered with the good
		// half of a damaged table would be read as "that profile was never
		// created", which is the reading that leads to publishing over it.
		if err := record.Verify(tenantID, profileID); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError(ctx, "list backend profiles", err, nil)
	}
	return records, nil
}

// lookup is the read path both readers share, including the integrity check.
func (r *ProfileRepository) lookup(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (storagebundle.ProfileRecord, error) {
	if err := contextError(ctx); err != nil {
		return storagebundle.ProfileRecord{}, err
	}
	if err := scope.Validate(); err != nil {
		return storagebundle.ProfileRecord{}, err
	}
	if err := tenant.ValidateResourceID("backend profile id", profileID); err != nil {
		return storagebundle.ProfileRecord{}, err
	}
	// Keyed by the caller's tenant rather than filtered after the lookup:
	// another tenant's profile is not found here, it is not reachable.
	record, rowTenantID, rowProfileID, err := scanProfile(
		r.pool.QueryRow(ctx, selectProfileSQL, scope.TenantID, profileID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return storagebundle.ProfileRecord{}, fmt.Errorf(
				"%w: %q", storagebundle.ErrProfileNotFound, profileID)
		}
		return storagebundle.ProfileRecord{}, storageError(ctx, "get backend profile", err, nil)
	}
	if err := record.Verify(rowTenantID, rowProfileID); err != nil {
		return storagebundle.ProfileRecord{}, err
	}
	return record, nil
}

// lockActiveTenant is the first gate of a profile write. It locks the tenant
// row, so the caller holds it for the rest of the transaction.
func (r *ProfileRepository) lockActiveTenant(
	ctx context.Context,
	tx pgx.Tx,
	tenantID string,
) error {
	var status string
	if err := tx.QueryRow(ctx, selectTenantForProfileWriteSQL, tenantID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("tenant", tenantID)
		}
		return storageError(ctx, "lock tenant for backend profile", err, nil)
	}
	if tenant.Status(status) != tenant.StatusActive {
		return fmt.Errorf(
			"%w: tenant %q has status %q", tenant.ErrTenantInactive, tenantID, status)
	}
	return nil
}

// scanProfile reads one row and returns the record beside the identity it was
// stored under.
//
// The two are returned separately on purpose: the row's tenant_id and id
// columns are how it was found, the decoded spec carries its own copy of both,
// and the caller compares them. A scan that merged the two would make the
// disagreement unobservable.
func scanProfile(row pgx.Row) (storagebundle.ProfileRecord, string, string, error) {
	var (
		record      storagebundle.ProfileRecord
		rowTenantID string
		rowID       string
		spec        []byte
	)
	if err := row.Scan(
		&rowTenantID, &rowID, &spec, &record.Fingerprint, &record.CreatedBy, &record.CreatedAt,
	); err != nil {
		return storagebundle.ProfileRecord{}, "", "", err
	}
	if err := decodeJSON(spec, &record.Profile); err != nil {
		return storagebundle.ProfileRecord{}, "", "", err
	}
	// pgx decodes timestamptz into time.Local. Normalising to UTC here keeps a
	// read equal to the write, which was UTC.
	record.CreatedAt = record.CreatedAt.UTC()
	return record, rowTenantID, rowID, nil
}
