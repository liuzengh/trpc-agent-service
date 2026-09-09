package storagemigration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// SQLStore persists jobs in the control-plane PostgreSQL database.
type SQLStore struct {
	DB  *sql.DB
	Now func() time.Time
}

var _ Store = (*SQLStore)(nil)

// NewJob constructs an opaque job ID and stable source-route fingerprint.
func NewJob(tenantID, appID string, version tenant.ConfigVersion, domain Domain, source, target tenant.BackendConfig, actor string) (Job, error) {
	if tenantID == "" || appID == "" || version == 0 || !validDomain(domain) || actor == "" {
		return Job{}, errors.New("storage migration: complete tenant, app, version, domain, and actor are required")
	}
	if !validRoutes(domain, source.Type, target.Type) {
		return Job{}, errors.New("storage migration: unsupported source or target route")
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return Job{}, errors.New("storage migration: generate job ID failed")
	}
	sourceJSON, _ := json.Marshal(source)
	// Include the trusted logical scope so identical external IDs in two
	// tenants can never collide in the shared migration ledger.
	digest := sha256.Sum256(append([]byte(tenantID+"\x00"+appID+"\x00"+string(domain)+"\x00"), sourceJSON...))
	return Job{TenantID: tenantID, JobID: hex.EncodeToString(random), AppID: appID, ConfigVersion: version, Domain: domain, Source: source.Clone(), Target: target.Clone(), SourceRouteHash: hex.EncodeToString(digest[:]), Status: StatusPending, CreatedBy: actor}, nil
}

func validRoutes(domain Domain, source, target tenant.BackendType) bool {
	switch domain {
	case DomainSession:
		return (source == tenant.BackendPostgres || source == tenant.BackendRedis) &&
			(target == tenant.BackendPostgres || target == tenant.BackendRedis)
	case DomainMemory:
		return source == tenant.BackendPostgres && (target == tenant.BackendPostgres || target == tenant.BackendExternal)
	case DomainArtifact:
		return (source == tenant.BackendPostgres || source == tenant.BackendS3) && (target == tenant.BackendPostgres || target == tenant.BackendS3)
	case DomainKnowledge:
		return (source == tenant.BackendPostgres || source == tenant.BackendQdrant) && (target == tenant.BackendPostgres || target == tenant.BackendQdrant)
	default:
		return false
	}
}

func (store *SQLStore) Create(ctx context.Context, job Job) (Job, error) {
	if store == nil || store.DB == nil || ctx == nil || job.TenantID == "" || job.JobID == "" || job.AppID == "" || job.ConfigVersion == 0 || !validDomain(job.Domain) || job.SourceRouteHash == "" || job.CreatedBy == "" {
		return Job{}, errors.New("storage migration: invalid SQL job")
	}
	source, _ := json.Marshal(job.Source)
	target, _ := json.Marshal(job.Target)
	now := store.now().UTC()
	_, err := store.DB.ExecContext(ctx, `INSERT INTO migration_jobs
		(tenant_id,job_id,app_id,domain,source_backend,target_backend,status,checkpoint_json,config_version,source_route_hash,created_by,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,'pending','{}'::jsonb,$7,$8,$9,$10,$10)`,
		job.TenantID, job.JobID, job.AppID, job.Domain, string(source), string(target), job.ConfigVersion, job.SourceRouteHash, job.CreatedBy, now)
	if err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) && pgError.Code == "23505" {
			return Job{}, ErrConflict
		}
		// Do not wrap driver text because it may contain connection metadata.
		return Job{}, errors.New("storage migration: create job failed")
	}
	return store.Get(ctx, job.TenantID, job.JobID)
}

func (store *SQLStore) List(ctx context.Context, tenantID string) ([]Job, error) {
	if store == nil || store.DB == nil || ctx == nil || tenantID == "" {
		return nil, errors.New("storage migration: tenant scope is required")
	}
	rows, err := store.DB.QueryContext(ctx, selectJobs+` WHERE tenant_id=$1 ORDER BY created_at DESC,job_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (store *SQLStore) Get(ctx context.Context, tenantID, jobID string) (Job, error) {
	if store == nil || store.DB == nil || ctx == nil || tenantID == "" || jobID == "" {
		return Job{}, ErrNotFound
	}
	job, err := scanJob(store.DB.QueryRowContext(ctx, selectJobs+` WHERE tenant_id=$1 AND job_id=$2`, tenantID, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

func (store *SQLStore) Claim(ctx context.Context, owner string, ttl time.Duration) (Job, bool, error) {
	if store == nil || store.DB == nil || ctx == nil || owner == "" || ttl <= 0 {
		return Job{}, false, errors.New("storage migration: claim owner and TTL are required")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback()
	now := store.now().UTC()
	row := tx.QueryRowContext(ctx, selectJobs+` WHERE
		(status='pending' OR (status='retrying' AND (retry_at IS NULL OR retry_at<=$1)) OR (status='running' AND (lease_until IS NULL OR lease_until<=$1)))
		ORDER BY created_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1`, now)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, tx.Commit()
	}
	if err != nil {
		return Job{}, false, err
	}
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return Job{}, false, err
	}
	job.ClaimOwner, job.ClaimToken = owner, hex.EncodeToString(tokenBytes)
	job.Attempts++
	job.LeaseUntil = now.Add(ttl)
	_, err = tx.ExecContext(ctx, `UPDATE migration_jobs SET status='running',attempts=$3,lease_owner=$4,claim_token=$5,lease_until=$6,retry_at=NULL,last_error=NULL,last_error_type=NULL,started_at=COALESCE(started_at,$7),updated_at=$7 WHERE tenant_id=$1 AND job_id=$2`, job.TenantID, job.JobID, job.Attempts, owner, job.ClaimToken, job.LeaseUntil, now)
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, err
	}
	job.Status, job.UpdatedAt = StatusRunning, now
	return job, true, nil
}

func (store *SQLStore) Save(ctx context.Context, job Job, progress Progress) error {
	status := StatusPending
	var completed any
	if progress.Done {
		status, completed = StatusCompleted, store.now().UTC()
	}
	result, err := store.DB.ExecContext(ctx, `UPDATE migration_jobs SET status=$6,checkpoint_json=$7,source_rows=$8,copied_rows=$9,lease_owner=NULL,claim_token=NULL,lease_until=NULL,completed_at=$10,updated_at=$5 WHERE tenant_id=$1 AND job_id=$2 AND lease_owner=$3 AND claim_token=$4 AND status='running'`, job.TenantID, job.JobID, job.ClaimOwner, job.ClaimToken, store.now().UTC(), status, nullJSON(progress.Checkpoint), progress.SourceRows, progress.CopiedRows, completed)
	return exact(result, err)
}

func (store *SQLStore) Fail(ctx context.Context, job Job, cause error, retryAt time.Time, maxAttempts int) error {
	status := StatusRetrying
	if job.Attempts >= maxAttempts {
		status = StatusFailed
	}
	errorType := fmt.Sprintf("%T", cause)
	result, err := store.DB.ExecContext(ctx, `UPDATE migration_jobs SET status=$6,retry_at=$7,last_error=$8,last_error_type=$8,lease_owner=NULL,claim_token=NULL,lease_until=NULL,updated_at=$5 WHERE tenant_id=$1 AND job_id=$2 AND lease_owner=$3 AND claim_token=$4 AND status='running'`, job.TenantID, job.JobID, job.ClaimOwner, job.ClaimToken, store.now().UTC(), status, retryAt.UTC(), errorType)
	return exact(result, err)
}

func (store *SQLStore) Cancel(ctx context.Context, tenantID, jobID string) error {
	result, err := store.DB.ExecContext(ctx, `UPDATE migration_jobs SET status='canceled',lease_owner=NULL,claim_token=NULL,lease_until=NULL,retry_at=NULL,updated_at=$3 WHERE tenant_id=$1 AND job_id=$2 AND status IN ('pending','retrying','failed')`, tenantID, jobID, store.now().UTC())
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrConflict
	}
	return nil
}

const selectJobs = `SELECT tenant_id,job_id,COALESCE(app_id,''),domain,source_backend,target_backend,status,COALESCE(checkpoint_json,'{}'::jsonb),COALESCE(config_version,0),COALESCE(source_route_hash,''),COALESCE(source_rows,0),COALESCE(copied_rows,0),COALESCE(attempts,0),COALESCE(lease_owner,''),COALESCE(claim_token,''),lease_until,COALESCE(last_error_type,''),COALESCE(created_by,''),created_at,updated_at,completed_at FROM migration_jobs`

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (Job, error) {
	var job Job
	var source, target []byte
	var lease sql.NullTime
	var completed sql.NullTime
	err := row.Scan(&job.TenantID, &job.JobID, &job.AppID, &job.Domain, &source, &target, &job.Status, &job.Checkpoint, &job.ConfigVersion, &job.SourceRouteHash, &job.SourceRows, &job.CopiedRows, &job.Attempts, &job.ClaimOwner, &job.ClaimToken, &lease, &job.LastErrorType, &job.CreatedBy, &job.CreatedAt, &job.UpdatedAt, &completed)
	if err != nil {
		return Job{}, err
	}
	if err := json.Unmarshal(source, &job.Source); err != nil {
		return Job{}, errors.New("storage migration: invalid stored source route")
	}
	if err := json.Unmarshal(target, &job.Target); err != nil {
		return Job{}, errors.New("storage migration: invalid stored target route")
	}
	if lease.Valid {
		job.LeaseUntil = lease.Time
	}
	if completed.Valid {
		job.CompletedAt = &completed.Time
	}
	return job, nil
}
func nullJSON(value []byte) any {
	if len(value) == 0 {
		return []byte(`{}`)
	}
	return value
}
func exact(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrClaim
	}
	return nil
}
func (store *SQLStore) now() time.Time {
	if store.Now != nil {
		return store.Now()
	}
	return time.Now()
}
