// Package migration provides tenant-scoped copy, dual-write, validation and
// cutover primitives for runtime backends.
package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
)

var (
	// ErrInvalid reports malformed migration input.
	ErrInvalid = errors.New("invalid migration request")
	// ErrConflict reports an invalid phase transition or missing barrier.
	ErrConflict = errors.New("migration conflict")
	// ErrValidation reports source/destination checksum divergence.
	ErrValidation = errors.New("migration validation failed")
	// ErrNotFound reports missing migration state.
	ErrNotFound = errors.New("migration record not found")
)

// Phase identifies a migration operation stage.
type Phase string

const (
	// PhaseDualWrite establishes the source write barrier.
	PhaseDualWrite Phase = "dual_write"
	// PhaseCopy copies the source snapshot after the barrier.
	PhaseCopy Phase = "copy"
	// PhaseCatchUp applies source changes after the initial watermark.
	PhaseCatchUp Phase = "catch_up"
	// PhaseValidate compares canonical source and destination digests.
	PhaseValidate Phase = "validate"
	// PhaseCutover switches a tenant to the destination backend.
	PhaseCutover Phase = "cutover"
	// PhaseRollback restores the pre-cutover backend.
	PhaseRollback Phase = "rollback"
)

// Backend identifies a migration route endpoint.
type Backend string

const (
	// BackendSource identifies the original provider.
	BackendSource Backend = "source"
	// BackendDestination identifies the target provider.
	BackendDestination Backend = "destination"
)

// Record is the provider-neutral migration unit. Payload is opaque to the
// tool and is never copied into logs or reports.
// Record is one tenant-scoped migration record.
type Record struct {
	TenantID string
	Kind     string
	Key      string
	Payload  []byte
	Version  int64
}

// Change is one monotonic source-log entry.
type Change struct {
	Sequence int64
	Record   Record
}

// Snapshot is a point-in-time record set and source watermark.
type Snapshot struct {
	Records   []Record
	Watermark int64
}

// Source reads snapshots and changes from the original backend.
type Source interface {
	BeginDualWrite(context.Context, string) (int64, error)
	Snapshot(context.Context, string) (Snapshot, error)
	Changes(context.Context, string, int64) ([]Change, int64, error)
}

// Destination applies records to the target backend.
type Destination interface {
	Apply(context.Context, []Record) error
	Snapshot(context.Context, string) (Snapshot, error)
}

// Router controls the tenant's active backend selection.
type Router interface {
	Current(context.Context, string) (Backend, error)
	Set(context.Context, string, Backend) error
}

// State is the durable phase state for one tenant migration. It contains no
// record payloads and can be persisted by an operator-owned SQL, Redis, or
// other strongly consistent adapter.
type State struct {
	TenantID        string
	Barrier         int64
	Copied          bool
	CaughtUp        bool
	PreviousBackend Backend
	Cutover         bool
}

// StateStore persists migration phase state independently from source and
// destination data. A Tool must not keep this state only in process memory.
type StateStore interface {
	Get(context.Context, string) (State, error)
	Put(context.Context, State) error
}

// Report summarizes one migration phase without payload contents.
type Report struct {
	TenantID             string
	Phase                Phase
	Copied               int
	CaughtUp             int
	SourceWatermark      int64
	DestinationWatermark int64
	SourceDigest         string
	DestinationDigest    string
	CutoverBackend       Backend
	RollbackAllowed      bool
	Validated            bool
}

// Tool orchestrates tenant-scoped migration phases.
type Tool struct {
	source      Source
	destination Destination
	router      Router
	state       StateStore
}

// NewToolWithStateStore creates a migration Tool with caller-owned durable
// phase state. New production integrations should use this constructor with a
// shared StateStore so another process can resume after a restart.
func NewToolWithStateStore(source Source, destination Destination, router Router, state StateStore) (*Tool, error) {
	if source == nil || destination == nil || router == nil || state == nil {
		return nil, ErrInvalid
	}
	return &Tool{source: source, destination: destination, router: router, state: state}, nil
}

// Begin establishes the source dual-write watermark barrier.
func (t *Tool) Begin(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	watermark, err := t.source.BeginDualWrite(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	if err := t.state.Put(ctx, State{TenantID: tenantID, Barrier: watermark}); err != nil {
		return Report{}, err
	}
	return Report{TenantID: tenantID, Phase: PhaseDualWrite, SourceWatermark: watermark}, nil
}

// Copy idempotently copies the source snapshot after the barrier.
func (t *Tool) Copy(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	state, ok, err := t.loadState(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	if !ok {
		return Report{}, ErrConflict
	}
	snapshot, err := t.source.Snapshot(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	if snapshot.Watermark < state.Barrier {
		return Report{}, ErrConflict
	}
	records := normalizeRecords(tenantID, snapshot.Records)
	if err := t.destination.Apply(ctx, records); err != nil {
		return Report{}, err
	}
	state.Copied = true
	state.CaughtUp = false
	if err := t.state.Put(ctx, state); err != nil {
		return Report{}, err
	}
	return Report{TenantID: tenantID, Phase: PhaseCopy, Copied: len(records), SourceWatermark: snapshot.Watermark}, nil
}

// CatchUp applies source changes after the initial watermark.
func (t *Tool) CatchUp(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	state, ok, err := t.loadState(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	barrier, copied := state.Barrier, state.Copied
	if !ok || !copied {
		return Report{}, ErrConflict
	}
	changes, watermark, err := t.source.Changes(ctx, tenantID, barrier)
	if err != nil {
		return Report{}, err
	}
	records := make([]Record, 0, len(changes))
	for _, change := range changes {
		if change.Sequence <= barrier {
			continue
		}
		records = append(records, change.Record)
	}
	if err := t.destination.Apply(ctx, normalizeRecords(tenantID, records)); err != nil {
		return Report{}, err
	}
	state.CaughtUp = true
	if err := t.state.Put(ctx, state); err != nil {
		return Report{}, err
	}
	return Report{TenantID: tenantID, Phase: PhaseCatchUp, CaughtUp: len(records), SourceWatermark: watermark, DestinationWatermark: watermark}, nil
}

// Validate compares canonical source and destination digests.
func (t *Tool) Validate(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	source, err := t.source.Snapshot(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	destination, err := t.destination.Snapshot(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	sourceDigest := Digest(source.Records)
	destinationDigest := Digest(destination.Records)
	report := Report{TenantID: tenantID, Phase: PhaseValidate, SourceWatermark: source.Watermark, DestinationWatermark: destination.Watermark, SourceDigest: sourceDigest, DestinationDigest: destinationDigest, Validated: sourceDigest == destinationDigest && len(normalizeRecords(tenantID, source.Records)) == len(normalizeRecords(tenantID, destination.Records))}
	if !report.Validated {
		return report, ErrValidation
	}
	return report, nil
}

// Cutover switches a validated tenant to the destination backend.
func (t *Tool) Cutover(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	state, stateKnown, err := t.loadState(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	if !stateKnown || !state.Copied || !state.CaughtUp {
		return Report{}, ErrConflict
	}
	validation, err := t.Validate(ctx, tenantID)
	if err != nil {
		return validation, err
	}
	previous, err := t.router.Current(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	if previous == BackendDestination {
		if !state.Cutover || state.PreviousBackend == BackendDestination || state.PreviousBackend == "" {
			return Report{}, ErrConflict
		}
		validation.Phase, validation.CutoverBackend, validation.RollbackAllowed = PhaseCutover, BackendDestination, true
		return validation, nil
	}
	if err := t.router.Set(ctx, tenantID, BackendDestination); err != nil {
		return Report{}, err
	}
	state.PreviousBackend = previous
	state.Cutover = true
	if err := t.state.Put(ctx, state); err != nil {
		// The route and phase marker form one operator-visible transition. If
		// durable state cannot record the cutover, restore the previous route so
		// a restart cannot observe an untracked destination.
		return Report{}, errors.Join(err, t.router.Set(context.Background(), tenantID, previous))
	}
	validation.Phase, validation.CutoverBackend, validation.RollbackAllowed = PhaseCutover, BackendDestination, true
	return validation, nil
}

// Rollback restores the pre-cutover backend after validation.
func (t *Tool) Rollback(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	state, ok, err := t.loadState(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	previous := state.PreviousBackend
	if !ok || !state.Cutover || previous == BackendDestination || previous == "" {
		return Report{}, ErrConflict
	}
	if _, err := t.Validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	if err := t.router.Set(ctx, tenantID, previous); err != nil {
		return Report{}, err
	}
	state.Cutover = false
	if err := t.state.Put(ctx, state); err != nil {
		// Keep the durable cutover marker truthful when rollback state cannot be
		// persisted. The destination is the only route that matches that marker.
		return Report{}, errors.Join(err, t.router.Set(context.Background(), tenantID, BackendDestination))
	}
	return Report{TenantID: tenantID, Phase: PhaseRollback, CutoverBackend: previous, RollbackAllowed: false, Validated: true}, nil
}

// Digest returns an order-independent SHA-256 digest of canonical records.
func Digest(records []Record) string {
	ordered := normalizeRecords("", records)
	hash := sha256.New()
	for _, record := range ordered {
		payload, _ := json.Marshal(struct {
			TenantID string `json:"tenant_id"`
			Kind     string `json:"kind"`
			Key      string `json:"key"`
			Payload  []byte `json:"payload"`
			Version  int64  `json:"version"`
		}{record.TenantID, record.Kind, record.Key, record.Payload, record.Version})
		_, _ = hash.Write(payload)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func normalizeRecords(tenantID string, records []Record) []Record {
	result := make([]Record, 0, len(records))
	for _, record := range records {
		if tenantID != "" {
			record.TenantID = tenantID
		}
		record.Payload = append([]byte(nil), record.Payload...)
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TenantID != result[j].TenantID {
			return result[i].TenantID < result[j].TenantID
		}
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		return result[i].Key < result[j].Key
	})
	return result
}

func validate(ctx context.Context, tenantID string) error {
	if ctx == nil || tenantID == "" {
		return ErrInvalid
	}
	return ctx.Err()
}

func (t *Tool) loadState(ctx context.Context, tenantID string) (State, bool, error) {
	if t == nil || t.state == nil {
		return State{}, false, ErrInvalid
	}
	state, err := t.state.Get(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	if state.TenantID != tenantID {
		return State{}, false, ErrConflict
	}
	return state, true, nil
}
