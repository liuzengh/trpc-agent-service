package datamigration

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrVectorRecordInvalid = errors.New("vector migration record is invalid")
	ErrRollbackConflict    = errors.New("vector migration rollback conflict")
)

// VectorRecord is the versioned JSONL interchange format. Content is
// optional for embedding-only exports; the content hash is always required.
type VectorRecord struct {
	FormatVersion   int            `json:"format_version"`
	TenantID        string         `json:"tenant_id"`
	AppName         string         `json:"app_name"`
	UserID          string         `json:"user_id"`
	SessionID       string         `json:"session_id"`
	SessionEpoch    string         `json:"session_epoch"`
	FilterKey       string         `json:"filter_key"`
	ThroughSequence int64          `json:"through_sequence"`
	SummaryVersion  int64          `json:"summary_version"`
	EmbeddingModel  string         `json:"embedding_model"`
	Dimension       int            `json:"dimension"`
	ContentSHA256   string         `json:"content_sha256"`
	Content         string         `json:"content,omitempty"`
	Name            string         `json:"name,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	Embedding       []float64      `json:"embedding"`
}

func (r VectorRecord) validate() error {
	if r.FormatVersion == 0 {
		r.FormatVersion = 1
	}
	if r.FormatVersion != 1 || r.TenantID == "" || r.AppName == "" || r.UserID == "" ||
		r.SessionID == "" || r.SessionEpoch == "" || r.ThroughSequence < 0 ||
		r.SummaryVersion <= 0 || r.EmbeddingModel == "" || r.Dimension <= 0 ||
		r.Dimension != len(r.Embedding) || len(r.ContentSHA256) != 64 {
		return ErrVectorRecordInvalid
	}
	for _, value := range r.Embedding {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return ErrVectorRecordInvalid
		}
	}
	if r.Content != "" && fmt.Sprintf("%x", sha256.Sum256([]byte(r.Content))) != r.ContentSHA256 {
		return fmt.Errorf("%w: content hash mismatch", ErrVectorRecordInvalid)
	}
	return nil
}

// DocumentID is independent of the summary body and therefore stable across
// retries and migration runs for one tenant/app/session/filter identity.
func (r VectorRecord) DocumentID() string {
	digest := sha256.Sum256([]byte(r.TenantID + "\x1f" + r.AppName + "\x1f" + r.SessionID + "\x1f" + r.FilterKey))
	return "session-summary-" + hex.EncodeToString(digest[:])
}

func VectorObjectHash(r VectorRecord) (string, error) {
	if err := r.validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(struct {
		TenantID        string         `json:"tenant_id"`
		AppName         string         `json:"app_name"`
		UserID          string         `json:"user_id"`
		SessionID       string         `json:"session_id"`
		SessionEpoch    string         `json:"session_epoch"`
		FilterKey       string         `json:"filter_key"`
		ThroughSequence int64          `json:"through_sequence"`
		SummaryVersion  int64          `json:"summary_version"`
		EmbeddingModel  string         `json:"embedding_model"`
		Dimension       int            `json:"dimension"`
		ContentSHA256   string         `json:"content_sha256"`
		Content         string         `json:"content"`
		Name            string         `json:"name"`
		Metadata        map[string]any `json:"metadata"`
		Embedding       []float64      `json:"embedding"`
	}{r.TenantID, r.AppName, r.UserID, r.SessionID, r.SessionEpoch, r.FilterKey,
		r.ThroughSequence, r.SummaryVersion, r.EmbeddingModel, r.Dimension,
		r.ContentSHA256, r.Content, r.Name, r.Metadata, r.Embedding})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// VectorSource is replayable from an opaque cursor.
type VectorSource interface {
	Scan(context.Context, string, int) ([]VectorRecord, string, bool, error)
}

type JSONLVectorSource struct {
	path string
}

func NewJSONLVectorSource(path string) (*JSONLVectorSource, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("vector JSONL source path is required")
	}
	if info, err := os.Stat(path); err != nil || info.IsDir() {
		return nil, errors.New("vector JSONL source is unavailable")
	}
	return &JSONLVectorSource{path: filepath.Clean(path)}, nil
}

func (s *JSONLVectorSource) Scan(ctx context.Context, cursor string, batchSize int) ([]VectorRecord, string, bool, error) {
	if batchSize <= 0 {
		return nil, cursor, false, errors.New("vector source batch size must be positive")
	}
	start, err := strconv.Atoi(cursor)
	if err != nil && cursor != "" {
		return nil, cursor, false, errors.New("vector source cursor is invalid")
	}
	file, err := os.Open(s.path)
	if err != nil {
		return nil, cursor, false, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 16<<20)
	line := 0
	result := make([]VectorRecord, 0, batchSize)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, cursor, false, err
		}
		if line < start {
			line++
			continue
		}
		line++
		var record VectorRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, strconv.Itoa(line), false, fmt.Errorf("decode vector JSONL line %d: %w", line, err)
		}
		if err := record.validate(); err != nil {
			return nil, strconv.Itoa(line), false, fmt.Errorf("validate vector JSONL line %d: %w", line, err)
		}
		result = append(result, record)
		if len(result) == batchSize {
			return result, strconv.Itoa(line), false, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, strconv.Itoa(line), false, err
	}
	return result, strconv.Itoa(line), true, nil
}

type VectorWriteResult struct {
	TargetHash string
	Status     ObjectStatus
	Conflict   string
}

type VectorTarget interface {
	Import(context.Context, string, VectorRecord) (VectorWriteResult, error)
	Verify(context.Context, VectorRecord) (VectorWriteResult, error)
	Rollback(context.Context, string, VectorRecord) (deleted bool, conflict bool, err error)
}

// PostgresVectorTarget is a pre-migrated pgvector target. All epoch and
// sequence guards are repeated in the SQL UPDATE predicate, so two migrators
// cannot race an older summary over a newer one.
type PostgresVectorTarget struct {
	pool      *pgxpool.Pool
	tableSQL  string
	tenantID  string
	appName   string
	dimension int
}

func NewPostgresVectorTarget(pool *pgxpool.Pool, table, tenantID, appName string, dimension int) (*PostgresVectorTarget, error) {
	if pool == nil || !safeVectorIdentifier(table) || tenantID == "" || appName == "" || dimension <= 0 {
		return nil, errors.New("vector target configuration is invalid")
	}
	return &PostgresVectorTarget{pool: pool, tableSQL: pgx.Identifier{table}.Sanitize(), tenantID: tenantID, appName: appName, dimension: dimension}, nil
}

func (t *PostgresVectorTarget) VerifySchema(ctx context.Context) error {
	var count int
	if err := t.pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = $1
		  AND column_name = ANY($2::text[])`, strings.Trim(t.tableSQL, `"`), []string{
		"tenant_id", "app_name", "document_id", "content_sha256", "metadata", "embedding",
		"source_session_epoch", "source_through_sequence", "source_summary_version",
		"embedding_model", "migration_id", "source_hash", "target_hash",
	}).Scan(&count); err != nil {
		return err
	}
	if count != 13 {
		return errors.New("vector target schema is missing migration metadata")
	}
	return nil
}

func (t *PostgresVectorTarget) Import(ctx context.Context, migrationID string, record VectorRecord) (VectorWriteResult, error) {
	if err := record.validate(); err != nil || record.TenantID != t.tenantID || record.AppName != t.appName || record.Dimension != t.dimension {
		return VectorWriteResult{}, ErrVectorRecordInvalid
	}
	sourceHash, err := VectorObjectHash(record)
	if err != nil {
		return VectorWriteResult{}, err
	}
	var existingEpoch, existingHash, existingMigration string
	var existingSequence int64
	err = t.pool.QueryRow(ctx, fmt.Sprintf(`SELECT source_session_epoch, source_through_sequence, source_hash, migration_id FROM %s WHERE tenant_id=$1 AND app_name=$2 AND document_id=$3`, t.tableSQL), t.tenantID, t.appName, record.DocumentID()).Scan(&existingEpoch, &existingSequence, &existingHash, &existingMigration)
	if err == nil {
		if existingEpoch != record.SessionEpoch || existingSequence > record.ThroughSequence || (existingSequence == record.ThroughSequence && existingHash != sourceHash) {
			return VectorWriteResult{TargetHash: existingHash, Status: ObjectMismatch, Conflict: "stale_source"}, nil
		}
		if existingHash == sourceHash {
			return VectorWriteResult{TargetHash: existingHash, Status: ObjectCopied}, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return VectorWriteResult{}, err
	}
	targetHash := sourceHash
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return VectorWriteResult{}, err
	}
	query := fmt.Sprintf(`
		INSERT INTO %s
			(tenant_id, app_name, document_id, name, content, content_sha256, metadata,
			 embedding, source_session_epoch, source_through_sequence, source_summary_version,
			 embedding_model, migration_id, source_hash, target_hash, deleted_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::vector,$9,$10,$11,$12,$13,$14,$15,NULL)
		ON CONFLICT (tenant_id, app_name, document_id) DO UPDATE SET
			name=EXCLUDED.name, content=EXCLUDED.content, content_sha256=EXCLUDED.content_sha256,
			metadata=EXCLUDED.metadata, embedding=EXCLUDED.embedding,
			source_session_epoch=EXCLUDED.source_session_epoch,
			source_through_sequence=EXCLUDED.source_through_sequence,
			source_summary_version=EXCLUDED.source_summary_version,
			embedding_model=EXCLUDED.embedding_model, migration_id=EXCLUDED.migration_id,
			source_hash=EXCLUDED.source_hash, target_hash=EXCLUDED.target_hash,
			updated_at=clock_timestamp(), deleted_at=NULL
		WHERE %s.source_session_epoch = EXCLUDED.source_session_epoch
		  AND %s.source_through_sequence <= EXCLUDED.source_through_sequence`, t.tableSQL, t.tableSQL, t.tableSQL)
	_, err = t.pool.Exec(ctx, query, t.tenantID, t.appName, record.DocumentID(), record.Name,
		record.Content, record.ContentSHA256, metadata, vectorLiteralForMigration(record.Embedding),
		record.SessionEpoch, record.ThroughSequence, record.SummaryVersion, record.EmbeddingModel,
		migrationID, sourceHash, targetHash)
	if err != nil {
		return VectorWriteResult{}, err
	}
	return VectorWriteResult{TargetHash: targetHash, Status: ObjectCopied}, nil
}

func (t *PostgresVectorTarget) Verify(ctx context.Context, record VectorRecord) (VectorWriteResult, error) {
	var targetHash string
	err := t.pool.QueryRow(ctx, fmt.Sprintf(`SELECT target_hash FROM %s WHERE tenant_id=$1 AND app_name=$2 AND document_id=$3 AND deleted_at IS NULL`, t.tableSQL), t.tenantID, t.appName, record.DocumentID()).Scan(&targetHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return VectorWriteResult{Status: ObjectMismatch, Conflict: "missing_target"}, nil
	}
	if err != nil {
		return VectorWriteResult{}, err
	}
	hash, err := VectorObjectHash(record)
	if err != nil {
		return VectorWriteResult{}, err
	}
	if targetHash != hash {
		return VectorWriteResult{TargetHash: targetHash, Status: ObjectMismatch, Conflict: "target_hash_mismatch"}, nil
	}
	return VectorWriteResult{TargetHash: targetHash, Status: ObjectVerified}, nil
}

func (t *PostgresVectorTarget) Rollback(ctx context.Context, migrationID string, record VectorRecord) (bool, bool, error) {
	hash, err := VectorObjectHash(record)
	if err != nil {
		return false, false, err
	}
	query := fmt.Sprintf(`DELETE FROM %s WHERE tenant_id=$1 AND app_name=$2 AND document_id=$3 AND migration_id=$4 AND source_hash=$5`, t.tableSQL)
	tag, err := t.pool.Exec(ctx, query, t.tenantID, t.appName, record.DocumentID(), migrationID, hash)
	if err != nil {
		return false, false, err
	}
	if tag.RowsAffected() == 1 {
		return true, false, nil
	}
	var exists bool
	err = t.pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE tenant_id=$1 AND app_name=$2 AND document_id=$3)`, t.tableSQL), t.tenantID, t.appName, record.DocumentID()).Scan(&exists)
	return false, exists, err
}

func vectorLiteralForMigration(values []float64) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.FormatFloat(value, 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func safeVectorIdentifier(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for i, r := range value {
		if (r >= 'a' && r <= 'z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}

type VectorOperator struct {
	source    VectorSource
	target    VectorTarget
	recorder  ObjectRecorder
	batchSize int
}

func NewVectorOperator(source VectorSource, target VectorTarget, recorder ObjectRecorder, batchSize int) (*VectorOperator, error) {
	if source == nil || target == nil || recorder == nil || batchSize <= 0 || batchSize > 10000 {
		return nil, errors.New("vector migration dependencies are required")
	}
	return &VectorOperator{source: source, target: target, recorder: recorder, batchSize: batchSize}, nil
}

func (o *VectorOperator) ExecutePhase(ctx context.Context, phase Phase, job Job) (Result, error) {
	if job.ResourceKind != "knowledge" {
		return Result{}, errors.New("vector operator received another resource kind")
	}
	switch phase {
	case PhasePrepare:
		return Result{Done: true}, nil
	case PhaseSnapshot, PhaseCatchUp:
		return o.copyBatch(ctx, job)
	case PhaseShadowRead, PhaseCanary, PhaseDrain, PhaseFinalize:
		return o.verifyBatch(ctx, job)
	case PhaseRollback:
		return o.rollbackBatch(ctx, job)
	default:
		return Result{}, errors.New("vector operator received invalid phase")
	}
}

func (o *VectorOperator) copyBatch(ctx context.Context, job Job) (Result, error) {
	records, next, done, err := o.source.Scan(ctx, checkpointCursor(job.Checkpoint), o.batchSize)
	if err != nil {
		return Result{}, err
	}
	result := Result{Done: done, Checkpoint: map[string]any{"cursor": next}}
	for _, record := range records {
		if record.TenantID != job.TenantID || record.AppName != job.AppName {
			return result, ErrVectorRecordInvalid
		}
		sourceHash, err := VectorObjectHash(record)
		if err != nil {
			return result, err
		}
		write, err := o.target.Import(ctx, job.MigrationID, record)
		if err != nil {
			_ = o.record(ctx, job, record, sourceHash, "", ObjectFailed, "")
			return result, err
		}
		status := write.Status
		if status == "" {
			status = ObjectCopied
		}
		if status == ObjectMismatch {
			result.Mismatches++
		}
		if err := o.record(ctx, job, record, sourceHash, write.TargetHash, status, write.Conflict); err != nil {
			return result, err
		}
		result.Copied++
	}
	return result, nil
}

func (o *VectorOperator) verifyBatch(ctx context.Context, job Job) (Result, error) {
	records, next, done, err := o.source.Scan(ctx, checkpointCursor(job.Checkpoint), o.batchSize)
	if err != nil {
		return Result{}, err
	}
	result := Result{Done: done, Checkpoint: map[string]any{"cursor": next}}
	for _, record := range records {
		sourceHash, err := VectorObjectHash(record)
		if err != nil {
			return result, err
		}
		write, err := o.target.Verify(ctx, record)
		if err != nil {
			return result, err
		}
		if write.Status == ObjectMismatch {
			result.Mismatches++
		}
		if err := o.record(ctx, job, record, sourceHash, write.TargetHash, write.Status, write.Conflict); err != nil {
			return result, err
		}
		result.Verified++
	}
	return result, nil
}

func (o *VectorOperator) rollbackBatch(ctx context.Context, job Job) (Result, error) {
	records, next, done, err := o.source.Scan(ctx, checkpointCursor(job.Checkpoint), o.batchSize)
	if err != nil {
		return Result{}, err
	}
	result := Result{Done: done, Checkpoint: map[string]any{"cursor": next}}
	for _, record := range records {
		hash, err := VectorObjectHash(record)
		if err != nil {
			return result, err
		}
		deleted, conflict, err := o.target.Rollback(ctx, job.MigrationID, record)
		if err != nil {
			return result, err
		}
		status := ObjectTombstoned
		rollbackState := "deleted"
		if conflict {
			status, rollbackState = ObjectMismatch, "conflict"
			result.Mismatches++
		} else if !deleted {
			rollbackState = "retained"
		}
		if err := o.record(ctx, job, record, hash, "", status, rollbackState); err != nil {
			return result, err
		}
	}
	if result.Mismatches > 0 {
		return result, ErrRollbackConflict
	}
	return result, nil
}

func (o *VectorOperator) record(ctx context.Context, job Job, record VectorRecord, sourceHash, targetHash string, status ObjectStatus, rollbackState string) error {
	if err := o.recorder.PutObject(ctx, ObjectRecord{
		MigrationID: job.MigrationID, ObjectKey: record.DocumentID(),
		SourceVer:  record.SessionEpoch + ":" + strconv.FormatInt(record.ThroughSequence, 10),
		SourceHash: sourceHash, TargetHash: targetHash, Status: status,
		SourceSessionEpoch: record.SessionEpoch, SourceThroughSequence: record.ThroughSequence,
		TargetVersion: record.EmbeddingModel, RollbackState: rollbackState,
	}); err != nil {
		return err
	}
	if reconciliation, ok := o.recorder.(ReconciliationRecorder); ok {
		reconciliationStatus := "matched"
		switch {
		case status == ObjectMismatch && rollbackState == "conflict":
			reconciliationStatus = "rollback_conflict"
		case status == ObjectMismatch:
			reconciliationStatus = "stale_source"
		case status == ObjectTombstoned:
			reconciliationStatus = "rollback_deleted"
		}
		if err := reconciliation.PutReconciliation(ctx, ReconciliationRecord{
			MigrationID: job.MigrationID, ObjectKey: record.DocumentID(), SourceHash: sourceHash,
			TargetHash: targetHash, Status: reconciliationStatus, Detail: rollbackState,
		}); err != nil {
			return err
		}
	}
	return nil
}

// MemoryVectorTarget is used by unit tests and local/demo migrations. It
// carries the same epoch/sequence guards as the PostgreSQL target.
type MemoryVectorTarget struct {
	values map[string]memoryVectorValue
}

type memoryVectorValue struct {
	migrationID string
	sourceHash  string
	targetHash  string
	epoch       string
	sequence    int64
}

func NewMemoryVectorTarget() *MemoryVectorTarget {
	return &MemoryVectorTarget{values: make(map[string]memoryVectorValue)}
}

func (t *MemoryVectorTarget) Import(_ context.Context, migrationID string, record VectorRecord) (VectorWriteResult, error) {
	hash, err := VectorObjectHash(record)
	if err != nil {
		return VectorWriteResult{}, err
	}
	key := record.DocumentID()
	if current, ok := t.values[key]; ok {
		if current.epoch != record.SessionEpoch || current.sequence > record.ThroughSequence || (current.sequence == record.ThroughSequence && current.sourceHash != hash) {
			return VectorWriteResult{TargetHash: current.targetHash, Status: ObjectMismatch, Conflict: "stale_source"}, nil
		}
		if current.sourceHash == hash {
			return VectorWriteResult{TargetHash: current.targetHash, Status: ObjectCopied}, nil
		}
	}
	t.values[key] = memoryVectorValue{migrationID: migrationID, sourceHash: hash, targetHash: hash, epoch: record.SessionEpoch, sequence: record.ThroughSequence}
	return VectorWriteResult{TargetHash: hash, Status: ObjectCopied}, nil
}

func (t *MemoryVectorTarget) Verify(_ context.Context, record VectorRecord) (VectorWriteResult, error) {
	hash, err := VectorObjectHash(record)
	if err != nil {
		return VectorWriteResult{}, err
	}
	current, ok := t.values[record.DocumentID()]
	if !ok || current.targetHash != hash {
		return VectorWriteResult{Status: ObjectMismatch, Conflict: "target_hash_mismatch"}, nil
	}
	return VectorWriteResult{TargetHash: current.targetHash, Status: ObjectVerified}, nil
}

func (t *MemoryVectorTarget) Rollback(_ context.Context, migrationID string, record VectorRecord) (bool, bool, error) {
	hash, err := VectorObjectHash(record)
	if err != nil {
		return false, false, err
	}
	current, ok := t.values[record.DocumentID()]
	if !ok {
		return false, false, nil
	}
	if current.migrationID != migrationID || current.sourceHash != hash {
		return false, true, nil
	}
	delete(t.values, record.DocumentID())
	return true, false, nil
}
