package sessiondriver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

func (d Driver) Repair(ctx context.Context, in RepairRequest) (RepairResult, error) {
	if d.Authority == nil || d.Ledger == nil || d.Source == nil || d.Target == nil {
		return RepairResult{}, runtime.ErrBackendUnavailable
	}
	current, err := d.Authority.Get(ctx, in.TenantID, in.MigrationID)
	if err != nil {
		return RepairResult{}, err
	}
	if err := validateWritableAuthority(current); err != nil || in.WorkerID == "" || in.Limit < 1 || in.Now.IsZero() || in.Lease <= 0 || in.RetryDelay < 0 {
		if err != nil {
			return RepairResult{}, err
		}
		return RepairResult{}, runtime.ErrInvariantViolation
	}
	claimed, err := d.Ledger.Claim(ctx, ClaimRequest{TenantID: in.TenantID, MigrationID: in.MigrationID,
		WorkerID: in.WorkerID, Limit: in.Limit, Now: in.Now, Lease: in.Lease})
	if err != nil {
		return RepairResult{}, err
	}
	result := RepairResult{Claimed: len(claimed)}
	for _, item := range claimed {
		applyErr := d.applyMutation(ctx, current, item, in.Now)
		if applyErr == nil {
			result.Applied++
			continue
		}
		_, retryErr := d.Ledger.MarkRetry(ctx, RetryRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
			MutationID: item.MutationID, WorkerID: in.WorkerID, SessionKey: item.SessionKey,
			ExpectedVersion: item.Version, ErrorClass: errorClass(applyErr), At: in.Now,
			NotBefore: in.Now.Add(in.RetryDelay)})
		if retryErr != nil {
			return result, retryErr
		}
		result.Retried++
	}
	return result, nil
}

func (d Driver) applyMutation(ctx context.Context, current migration.Migration, item Mutation, at time.Time) error {
	if item.TenantID != current.TenantID || item.MigrationID != current.MigrationID || item.Epoch != current.Epoch ||
		item.State != MutationApplying || item.SourceVersion < 1 {
		return runtime.ErrInvariantViolation
	}
	reader, writer := d.Source, d.Target
	if item.Direction == DirectionReverse {
		reader, writer = d.ReverseSource, d.ReverseTarget
	} else if item.Direction != DirectionForward {
		return runtime.ErrInvariantViolation
	}
	if reader == nil || writer == nil {
		return runtime.ErrBackendUnavailable
	}
	snapshot, err := reader.LoadSessionImage(ctx, item.SessionKey)
	if err != nil {
		return err
	}
	if snapshot.Head.TenantID != item.TenantID || snapshot.Head.Version < item.SourceVersion {
		return runtime.ErrInvariantViolation
	}
	digest, err := SnapshotDigest(snapshot)
	if err != nil {
		return err
	}
	applied, err := writer.ApplySessionSnapshot(ctx, ApplyRequest{TenantID: item.TenantID,
		MigrationID: item.MigrationID, MutationID: item.MutationID, Epoch: item.Epoch,
		Image: snapshot, SnapshotDigest: digest})
	if err != nil {
		return err
	}
	if applied.SessionVersion < snapshot.Head.Version ||
		(applied.SessionVersion == snapshot.Head.Version && applied.SnapshotDigest != digest) || !validDigest(applied.SnapshotDigest) {
		return runtime.ErrInvariantViolation
	}
	_, err = d.Ledger.MarkApplied(ctx, CompleteRequest{TenantID: item.TenantID, MigrationID: item.MigrationID,
		MutationID: item.MutationID, WorkerID: item.LeaseOwner, SessionKey: item.SessionKey,
		ExpectedVersion: item.Version, TargetVersion: applied.SessionVersion, TargetDigest: applied.SnapshotDigest,
		At: at})
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
	page, err := d.Backfill.PageSessions(ctx, PageRequest{TenantID: current.TenantID,
		SnapshotWatermark: current.SnapshotWatermark, After: current.BackfillCheckpoint, Limit: in.Limit})
	if err != nil {
		return migration.BatchResult{}, err
	}
	if page.NextCheckpoint == "" || page.NextCheckpoint == current.BackfillCheckpoint || (len(page.Sessions) == 0 && !page.Complete) {
		return migration.BatchResult{}, runtime.ErrInvariantViolation
	}
	fingerprints := make([]string, 0, len(page.Sessions))
	for _, snapshot := range page.Sessions {
		if snapshot.Head.TenantID != current.TenantID {
			return migration.BatchResult{}, runtime.ErrTenantScope
		}
		digest, err := SnapshotDigest(snapshot)
		if err != nil {
			return migration.BatchResult{}, err
		}
		mutationID := stableID("backfill", current.MigrationID, snapshot.Head.AgentAppID, snapshot.Head.SessionID,
			strconv.FormatInt(snapshot.Head.Version, 10))
		applied, err := d.Target.ApplySessionSnapshot(ctx, ApplyRequest{TenantID: current.TenantID,
			MigrationID: current.MigrationID, MutationID: mutationID, Epoch: current.Epoch,
			Image: snapshot, SnapshotDigest: digest})
		if err != nil {
			return migration.BatchResult{}, err
		}
		if applied.SessionVersion < snapshot.Head.Version ||
			(applied.SessionVersion == snapshot.Head.Version && applied.SnapshotDigest != digest) || !validDigest(applied.SnapshotDigest) {
			return migration.BatchResult{}, runtime.ErrInvariantViolation
		}
		fingerprints = append(fingerprints, snapshot.Head.AgentAppID+"\x00"+snapshot.Head.SessionID+"\x00"+
			strconv.FormatInt(snapshot.Head.Version, 10)+"\x00"+digest)
	}
	sort.Strings(fingerprints)
	batchDigest := hashStrings(fingerprints...)
	batchID := stableID("batch", current.MigrationID, strconv.FormatInt(current.NextBatchSeq, 10),
		current.BackfillCheckpoint, page.NextCheckpoint, batchDigest)
	return d.Authority.CommitBatch(ctx, migration.BatchRequest{TenantID: current.TenantID,
		MigrationID: current.MigrationID, BatchID: batchID, Epoch: current.Epoch,
		ExpectedVersion: current.Version, BatchSeq: current.NextBatchSeq,
		FromCheckpoint: current.BackfillCheckpoint, ToCheckpoint: page.NextCheckpoint,
		Digest: batchDigest, RecordCount: int64(len(page.Sessions)), Complete: page.Complete, CommittedAt: in.At})
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
	if d.Authority == nil || d.Ledger == nil || d.SourceInventory == nil || d.TargetInventory == nil {
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
	if err != nil || outstanding != 0 {
		if err != nil {
			return migration.Verification{}, err
		}
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
		source.Digest != target.Digest || source.Watermark != target.Watermark || source.SampleDigest != target.SampleDigest ||
		!validDigest(source.Digest) || !validDigest(source.SampleDigest) || source.Watermark == "" {
		return migration.Verification{}, runtime.ErrInvariantViolation
	}
	return migration.Verification{SourceCount: source.Count, TargetCount: target.Count,
		SourceDigest: source.Digest, TargetDigest: target.Digest, SourceWatermark: source.Watermark,
		TargetWatermark: target.Watermark, SampleDigest: source.SampleDigest}, nil
}

func SnapshotDigest(snapshot SessionImage) (string, error) {
	if snapshot.Head.TenantID == "" || snapshot.Head.AgentAppID == "" || snapshot.Head.SessionID == "" ||
		snapshot.Head.Version < 0 || snapshot.Head.NextInputSeq < 1 ||
		snapshot.LastAllocatedInputSeq+1 < snapshot.Head.NextInputSeq || snapshot.Head.Version != int64(len(snapshot.Commits)) {
		return "", runtime.ErrInvariantViolation
	}
	for index, commit := range snapshot.Commits {
		if commit.CommitID == "" || commit.RequestID == "" || !validDigest(commit.RequestDigest) || commit.Stage == "" ||
			commit.InputSeq < 1 || commit.Fence < 1 || commit.SessionVersion < 1 || commit.CreatedAt.IsZero() ||
			commit.SessionVersion != int64(index+1) || !validSessionOutcome(commit.Outcome) {
			return "", runtime.ErrInvariantViolation
		}
	}
	if sdk := snapshot.SDK; sdk != nil {
		if sdk.AppName != snapshot.Head.TenantID+"/"+snapshot.Head.AgentAppID || sdk.UserID == "" ||
			sdk.SessionID != snapshot.Head.SessionID || !json.Valid(sdk.State) || sdk.CreatedAt.IsZero() || sdk.UpdatedAt.IsZero() {
			return "", runtime.ErrInvariantViolation
		}
		for _, event := range sdk.Events {
			if !json.Valid(event.Event) || event.CreatedAt.IsZero() || event.UpdatedAt.IsZero() {
				return "", runtime.ErrInvariantViolation
			}
		}
		for _, event := range sdk.TrackEvents {
			if event.Track == "" || !json.Valid(event.Event) || event.CreatedAt.IsZero() || event.UpdatedAt.IsZero() {
				return "", runtime.ErrInvariantViolation
			}
		}
		for _, summary := range sdk.Summaries {
			if !json.Valid(summary.Summary) || summary.UpdatedAt.IsZero() {
				return "", runtime.ErrInvariantViolation
			}
		}
	}
	canonical := struct {
		Key                sessionstore.SessionKey `json:"key"`
		Version            int64                   `json:"version"`
		LastFence          uint64                  `json:"last_fence"`
		NextInput          uint64                  `json:"next_input_seq"`
		LastAllocatedInput uint64                  `json:"last_allocated_input_seq"`
		SDK                *SDKSessionImage        `json:"sdk,omitempty"`
		Commits            []CommitRecord          `json:"commits"`
	}{snapshot.Head.SessionKey, snapshot.Head.Version, snapshot.Head.LastFence,
		snapshot.Head.NextInputSeq, snapshot.LastAllocatedInputSeq, snapshot.SDK, snapshot.Commits}
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func validateWritableAuthority(value migration.Migration) error {
	if value.Domain != Domain {
		return runtime.ErrInvariantViolation
	}
	switch value.State {
	case migration.StateDualWrite, migration.StateBackfill, migration.StateVerify, migration.StateCutover, migration.StateObserve:
		return nil
	default:
		return runtime.ErrInvariantViolation
	}
}

func stableID(parts ...string) string { return "sm_" + hashStrings(parts...) }

func FingerprintFromItems(items []string, watermark string) Fingerprint {
	ordered := append([]string(nil), items...)
	sort.Strings(ordered)
	sample := ordered
	if len(sample) > 16 {
		sample = append(append([]string(nil), sample[:8]...), sample[len(sample)-8:]...)
	}
	return Fingerprint{Count: int64(len(ordered)), Digest: hashStrings(ordered...),
		Watermark: watermark, SampleDigest: hashStrings(sample...)}
}

func hashStrings(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(h, "%d:", len(part))
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validSessionOutcome(value runtime.Outcome) bool {
	switch value {
	case runtime.OutcomePending, runtime.OutcomeQueued, runtime.OutcomeRunning,
		runtime.OutcomeWaitingConfirmation, runtime.OutcomeSucceeded, runtime.OutcomeDenied,
		runtime.OutcomeFailed, runtime.OutcomeCancelled, runtime.OutcomeConfirmationDenied,
		runtime.OutcomeConfirmationTimeout:
		return true
	default:
		return false
	}
}

func errorClass(err error) string {
	switch {
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
