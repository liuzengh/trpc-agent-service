package knowledgedriver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func ImageDigest(image ChunkImage) (string, error) {
	if err := validateImage(image); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(image)
	if err != nil {
		return "", runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func (d Driver) Repair(ctx context.Context, in RepairRequest) (RepairResult, error) {
	if d.Authority == nil || d.Ledger == nil || d.Source == nil || d.Target == nil {
		return RepairResult{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, in.TenantID, in.MigrationID)
	if err != nil {
		return RepairResult{}, err
	}
	if err := writable(current); err != nil {
		return RepairResult{}, err
	}
	if in.WorkerID == "" || in.Limit < 1 || in.Now.IsZero() || in.Lease <= 0 || in.RetryDelay < 0 {
		return RepairResult{}, runtime.ErrInvariantViolation
	}
	claimed, err := d.Ledger.Claim(ctx, ClaimRequest{TenantID: in.TenantID, MigrationID: in.MigrationID,
		WorkerID: in.WorkerID, Limit: in.Limit, Now: in.Now, Lease: in.Lease})
	if err != nil {
		return RepairResult{}, err
	}
	result := RepairResult{Claimed: len(claimed)}
	for _, item := range claimed {
		applyErr := d.apply(ctx, current, item, in.Now)
		if applyErr == nil {
			result.Applied++
			continue
		}
		_, retryErr := d.Ledger.MarkRetry(ctx, RetryRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
			MutationID: item.MutationID, WorkerID: item.LeaseOwner, Key: item.Key, ExpectedVersion: item.Version,
			ErrorClass: classify(applyErr), At: in.Now, NotBefore: in.Now.Add(in.RetryDelay)})
		if retryErr != nil {
			return result, retryErr
		}
		result.Retried++
	}
	return result, nil
}

func (d Driver) apply(ctx context.Context, current migration.Migration, item Mutation, at time.Time) error {
	if item.TenantID != current.TenantID || item.MigrationID != current.MigrationID || item.Epoch != current.Epoch ||
		item.State != MutationApplying || item.Key.TenantID != item.TenantID || item.SourceRevision < 1 {
		return runtime.ErrInvariantViolation
	}
	reader, writer := d.Source, d.Target
	if item.Direction == DirectionReverse {
		reader, writer = d.ReverseSource, d.ReverseReplica
	} else if item.Direction != DirectionForward {
		return runtime.ErrInvariantViolation
	}
	if reader == nil || writer == nil {
		return runtime.ErrBackendUnavailable
	}
	image, err := reader.LoadChunk(ctx, item.Key)
	if err != nil {
		return err
	}
	digest, err := ImageDigest(image)
	if err != nil {
		return err
	}
	if image.Key != item.Key || image.Operation != item.Operation || image.Revision < item.SourceRevision ||
		(image.Revision == item.SourceRevision && digest != item.MutationDigest) {
		return runtime.ErrInvariantViolation
	}
	applied, err := writer.ApplyChunk(ctx, ApplyRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
		MutationID: item.MutationID, Epoch: item.Epoch, Image: image, ImageDigest: digest})
	if err != nil {
		return err
	}
	if applied.Revision < image.Revision || (applied.Revision == image.Revision && applied.Digest != digest) || !validDigest(applied.Digest) {
		return runtime.ErrInvariantViolation
	}
	_, err = d.Ledger.MarkApplied(ctx, CompleteRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
		MutationID: item.MutationID, WorkerID: item.LeaseOwner, Key: item.Key, ExpectedVersion: item.Version,
		TargetRevision: applied.Revision, TargetDigest: applied.Digest, At: at})
	return err
}

func (d Driver) BackfillOnce(ctx context.Context, in BackfillRequest) (migration.BatchResult, error) {
	if d.Authority == nil || d.Backfill == nil || d.Target == nil {
		return migration.BatchResult{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, in.TenantID, in.MigrationID)
	if err != nil {
		return migration.BatchResult{}, err
	}
	if current.Domain != Domain || current.State != migration.StateBackfill || current.BackfillComplete || in.Limit < 1 || in.At.IsZero() {
		return migration.BatchResult{}, runtime.ErrInvariantViolation
	}
	page, err := d.Backfill.PageChunks(ctx, PageRequest{TenantID: current.TenantID,
		SnapshotWatermark: current.SnapshotWatermark, After: current.BackfillCheckpoint, Limit: in.Limit})
	if err != nil {
		return migration.BatchResult{}, err
	}
	if page.NextCheckpoint == "" || page.NextCheckpoint == current.BackfillCheckpoint || (len(page.Chunks) == 0 && !page.Complete) {
		return migration.BatchResult{}, runtime.ErrInvariantViolation
	}
	fingerprints := make([]string, 0, len(page.Chunks))
	previous := current.BackfillCheckpoint
	for _, image := range page.Chunks {
		if image.Key.TenantID != current.TenantID {
			return migration.BatchResult{}, runtime.ErrTenantScope
		}
		coordinate := checkpoint(image.Key)
		if coordinate <= previous {
			return migration.BatchResult{}, runtime.ErrInvariantViolation
		}
		previous = coordinate
		digest, err := ImageDigest(image)
		if err != nil {
			return migration.BatchResult{}, err
		}
		mutationID := stableID("backfill", current.MigrationID, coordinate, strconv.FormatInt(image.Revision, 10))
		applied, err := d.Target.ApplyChunk(ctx, ApplyRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
			MutationID: mutationID, Epoch: current.Epoch, Image: image, ImageDigest: digest})
		if err != nil {
			return migration.BatchResult{}, err
		}
		if applied.Revision < image.Revision || (applied.Revision == image.Revision && applied.Digest != digest) || !validDigest(applied.Digest) {
			return migration.BatchResult{}, runtime.ErrInvariantViolation
		}
		fingerprints = append(fingerprints, coordinate+"\x00"+strconv.FormatInt(image.Revision, 10)+"\x00"+digest)
	}
	if len(page.Chunks) > 0 && page.NextCheckpoint != previous {
		return migration.BatchResult{}, runtime.ErrInvariantViolation
	}
	batchDigest := hashStrings(fingerprints...)
	batchID := stableID("batch", current.MigrationID, strconv.FormatInt(current.NextBatchSeq, 10),
		current.BackfillCheckpoint, page.NextCheckpoint, batchDigest)
	return d.Authority.CommitBatch(ctx, migration.BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID,
		BatchID: batchID, Epoch: current.Epoch, ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq,
		FromCheckpoint: current.BackfillCheckpoint, ToCheckpoint: page.NextCheckpoint, Digest: batchDigest,
		RecordCount: int64(len(page.Chunks)), Complete: page.Complete, CommittedAt: in.At})
}

func (d Driver) EnterVerify(ctx context.Context, tenantID, migrationID string, at time.Time) (migration.Migration, error) {
	if d.Authority == nil || d.Ledger == nil {
		return migration.Migration{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Migration{}, err
	}
	if current.Domain != Domain || current.State != migration.StateBackfill || !current.BackfillComplete || at.IsZero() {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	outstanding, err := d.Ledger.Outstanding(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Migration{}, err
	}
	if outstanding != 0 {
		return migration.Migration{}, runtime.ErrInvariantViolation
	}
	return d.Authority.Transition(ctx, migration.TransitionRequest{TenantID: tenantID, MigrationID: migrationID,
		ExpectedVersion: current.Version, To: migration.StateVerify, At: at})
}

func (d Driver) ShadowVerify(ctx context.Context, tenantID, migrationID string) (migration.Verification, error) {
	if d.Authority == nil || d.Ledger == nil || d.SourceInventory == nil || d.TargetInventory == nil ||
		d.ProbeSource == nil || d.SearchTarget == nil {
		return migration.Verification{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Verification{}, err
	}
	if current.Domain != Domain || current.State != migration.StateVerify {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	outstanding, err := d.Ledger.Outstanding(ctx, tenantID, migrationID)
	if err != nil {
		return migration.Verification{}, err
	}
	if outstanding != 0 {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	source, err := d.SourceInventory.Fingerprint(ctx, tenantID, "")
	if err != nil {
		return migration.Verification{}, err
	}
	if source.Watermark == "" {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	target, err := d.TargetInventory.Fingerprint(ctx, tenantID, source.Watermark)
	if err != nil {
		return migration.Verification{}, err
	}
	if source.DivergenceCount != 0 || target.DivergenceCount != 0 || source.Count != target.Count ||
		source.Digest != target.Digest || source.Watermark != target.Watermark || !validDigest(source.Digest) {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	probes, err := d.ProbeSource.Probes(ctx, tenantID, source.Watermark)
	if err != nil {
		return migration.Verification{}, err
	}
	sampleDigest, err := d.verifyProbes(ctx, tenantID, probes)
	if err != nil {
		return migration.Verification{}, err
	}
	return migration.Verification{SourceCount: source.Count, TargetCount: target.Count, SourceDigest: source.Digest,
		TargetDigest: target.Digest, SourceWatermark: source.Watermark, TargetWatermark: target.Watermark,
		SampleDigest: sampleDigest}, nil
}

func (d Driver) verifyProbes(ctx context.Context, tenantID string, probes []Probe) (string, error) {
	if len(probes) == 0 {
		return "", runtime.ErrInvariantViolation
	}
	sort.Slice(probes, func(i, j int) bool { return probes[i].ProbeID < probes[j].ProbeID })
	evidence := make([]string, 0, len(probes))
	lastID := ""
	for _, probe := range probes {
		if probe.ProbeID == "" || probe.ProbeID == lastID || probe.TenantID != tenantID || probe.KnowledgeID == "" ||
			probe.KnowledgeVersion < 1 || probe.Query == "" || len(probe.Expected) == 0 || probe.MinRecallPPM < 1 || probe.MinRecallPPM > 1_000_000 {
			return "", runtime.ErrInvariantViolation
		}
		lastID = probe.ProbeID
		expected := make(map[ChunkKey]struct{}, len(probe.Expected))
		expectedCoordinates := make([]string, 0, len(probe.Expected))
		for _, key := range probe.Expected {
			if key.TenantID != tenantID || key.KnowledgeID != probe.KnowledgeID || key.KnowledgeVersion != probe.KnowledgeVersion || key.ChunkID == "" {
				return "", runtime.ErrTenantScope
			}
			if _, duplicate := expected[key]; duplicate {
				return "", runtime.ErrInvariantViolation
			}
			expected[key] = struct{}{}
			expectedCoordinates = append(expectedCoordinates, checkpoint(key))
		}
		sort.Strings(expectedCoordinates)
		hits, err := d.SearchTarget.Search(ctx, SearchRequest{TenantID: tenantID, KnowledgeID: probe.KnowledgeID,
			KnowledgeVersion: probe.KnowledgeVersion, Query: probe.Query})
		if err != nil {
			return "", err
		}
		seen := make(map[ChunkKey]struct{}, len(hits))
		hitCoordinates := make([]string, 0, len(hits))
		matched := int64(0)
		for _, hit := range hits {
			if hit.TenantID != tenantID || hit.KnowledgeID != probe.KnowledgeID || hit.KnowledgeVersion != probe.KnowledgeVersion || hit.ChunkID == "" {
				return "", runtime.ErrTenantScope
			}
			if _, duplicate := seen[hit]; duplicate {
				return "", runtime.ErrInvariantViolation
			}
			seen[hit] = struct{}{}
			hitCoordinates = append(hitCoordinates, checkpoint(hit))
			if _, ok := expected[hit]; ok {
				matched++
			}
		}
		recall := matched * 1_000_000 / int64(len(expected))
		if recall < probe.MinRecallPPM {
			return "", runtime.ErrInvariantViolation
		}
		sort.Strings(hitCoordinates)
		evidence = append(evidence, strings.Join([]string{probe.ProbeID, hashStrings(probe.Query),
			hashStrings(expectedCoordinates...), hashStrings(hitCoordinates...), strconv.FormatInt(matched, 10),
			strconv.FormatInt(recall, 10)}, "\x00"))
	}
	return hashStrings(evidence...), nil
}

func validateImage(image ChunkImage) error {
	key := image.Key
	if key.TenantID == "" || key.KnowledgeID == "" || key.KnowledgeVersion < 1 || key.ChunkID == "" || image.Revision < 1 ||
		(image.Operation != OperationUpsert && image.Operation != OperationDelete) || !validDigest(image.SourceDigest) ||
		!validDigest(image.ContentDigest) || !validDigest(image.MetadataDigest) || image.EmbeddingProfileID == "" ||
		image.EmbeddingVersion < 1 || image.VectorGeneration == "" {
		return runtime.ErrInvariantViolation
	}
	if image.Operation == OperationDelete {
		if image.Content != "" || len(image.Metadata) != 0 || len(image.Vector) != 0 {
			return runtime.ErrInvariantViolation
		}
		return nil
	}
	if image.Content == "" || len(image.Vector) == 0 {
		return runtime.ErrInvariantViolation
	}
	contentSum := sha256.Sum256([]byte(image.Content))
	if image.ContentDigest != hex.EncodeToString(contentSum[:]) {
		return runtime.ErrInvariantViolation
	}
	for key, value := range image.Metadata {
		if key == "" || strings.ContainsAny(key+value, "\x00\r\n") {
			return runtime.ErrInvariantViolation
		}
	}
	for _, value := range image.Vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return runtime.ErrInvariantViolation
		}
	}
	metadata, err := json.Marshal(image.Metadata)
	if err != nil {
		return runtime.ErrInvariantViolation
	}
	metadataSum := sha256.Sum256(metadata)
	if image.MetadataDigest != hex.EncodeToString(metadataSum[:]) {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func writable(current migration.Migration) error {
	if current.Domain != Domain {
		return runtime.ErrInvariantViolation
	}
	switch current.State {
	case migration.StatePlanned, migration.StateSnapshot, migration.StateDualWrite, migration.StateBackfill,
		migration.StateVerify, migration.StateCutover, migration.StateObserve:
		return nil
	default:
		return runtime.ErrInvariantViolation
	}
}

func checkpoint(key ChunkKey) string {
	return strings.Join([]string{hex.EncodeToString([]byte(key.KnowledgeID)), fmt.Sprintf("%020d", key.KnowledgeVersion),
		hex.EncodeToString([]byte(key.ChunkID))}, ":")
}
func stableID(parts ...string) string { return hashStrings(parts...) }
func hashStrings(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func classify(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, runtime.ErrInvariantViolation):
		return "invariant"
	case errors.Is(err, runtime.ErrTenantScope):
		return "tenant_scope"
	default:
		return "target_unavailable"
	}
}
