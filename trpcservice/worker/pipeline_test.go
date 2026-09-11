package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/jackc/pgx/v5"
)

type pipelineTestParticipant struct{}

func (*pipelineTestParticipant) CompleteInTransaction(context.Context, pgx.Tx, AtomicDeliveryPlan) error {
	return nil
}

func (*pipelineTestParticipant) MarkCommitted() {}

func TestDurablePipelineMetadataDistinguishesLegacyPayload(t *testing.T) {
	var legacy struct {
		Pipeline DurablePipelineMetadata `json:"pipeline"`
	}
	if err := json.Unmarshal([]byte(`{}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if !legacy.Pipeline.IsLegacy() {
		t.Fatalf("missing metadata must be legacy: %+v", legacy.Pipeline)
	}
	current := DurablePipelineMetadata{
		SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitDisabled,
	}
	if current.IsLegacy() {
		t.Fatalf("current disabled pipeline reported legacy: %+v", current)
	}
}

func TestDurablePipelineMetadataValidation(t *testing.T) {
	validIdentity := "postgres-binding-v1:digest"
	for name, test := range map[string]struct {
		metadata DurablePipelineMetadata
		wantErr  bool
	}{
		"legacy": {},
		"current disabled": {
			metadata: DurablePipelineMetadata{SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitDisabled},
		},
		"current required": {
			metadata: DurablePipelineMetadata{SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitRequired, DatabaseIdentity: validIdentity},
		},
		"future": {
			metadata: DurablePipelineMetadata{SchemaVersion: DurablePipelineVersion + 1, AtomicCommitMode: AtomicCommitDisabled}, wantErr: true,
		},
		"required missing identity": {
			metadata: DurablePipelineMetadata{SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitRequired}, wantErr: true,
		},
		"disabled with identity": {
			metadata: DurablePipelineMetadata{SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitDisabled, DatabaseIdentity: validIdentity}, wantErr: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := test.metadata.Validate()
			if test.wantErr != (err != nil) {
				t.Fatalf("Validate() = %v, wantErr %v", err, test.wantErr)
			}
			if err != nil && !errors.Is(err, ErrUnsupportedPipeline) {
				t.Fatalf("Validate() = %v, want ErrUnsupportedPipeline", err)
			}
		})
	}
}

func TestValidateAtomicRuntimeFailsClosed(t *testing.T) {
	identity := "postgres-binding-v1:queue"
	required := Task{
		Tenant: config.TenantConfig{Data: config.DataConfig{Session: config.BackendConfig{Type: "sql"}}},
		Pipeline: DurablePipelineMetadata{
			SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitRequired,
			DatabaseIdentity: identity,
		},
		TurnCommitParticipant: &pipelineTestParticipant{},
	}
	if err := validateAtomicRuntime(required, identity); err != nil {
		t.Fatalf("valid required runtime: %v", err)
	}
	if err := validateAtomicRuntime(required, "postgres-binding-v1:other"); !errors.Is(err, ErrAtomicDatabaseMismatch) {
		t.Fatalf("mismatched runtime = %v, want ErrAtomicDatabaseMismatch", err)
	}
	withoutParticipant := required
	withoutParticipant.TurnCommitParticipant = nil
	if err := validateAtomicRuntime(withoutParticipant, identity); !errors.Is(err, ErrAtomicCommitUnavailable) {
		t.Fatalf("missing participant = %v, want ErrAtomicCommitUnavailable", err)
	}
	legacy := Task{TurnCommitParticipant: &pipelineTestParticipant{}}
	if err := validateAtomicRuntime(legacy, ""); !errors.Is(err, ErrAtomicCommitUnavailable) {
		t.Fatalf("legacy participant = %v, want ErrAtomicCommitUnavailable", err)
	}
}
