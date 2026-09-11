package application

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
)

func TestActualPublicationOutputSatisfiesClosedEventAndManifestSchemas(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	result, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner",
		IdempotencyKey: "schema-publication", Input: deploymentInput(),
	})
	if err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	const manifestURL = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest.schema.json"
	const eventURL = "https://jfsas.dev/events/control/v1/runtime-manifest-published.schema.json"
	for location, raw := range map[string][]byte{
		manifestURL: deploymentv1.ManifestSchema,
		eventURL:    controleventsv1.RuntimeManifestPublishedSchema,
	} {
		var document any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		if err := compiler.AddResource(location, document); err != nil {
			t.Fatal(err)
		}
	}
	for location, raw := range map[string]json.RawMessage{
		eventURL:    h.store.lastEvent.Payload,
		manifestURL: mustJSON(t, result.Published.Manifest),
	} {
		schema, err := compiler.Compile(location)
		if err != nil {
			t.Fatal(err)
		}
		var document any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(document); err != nil {
			t.Fatalf("actual publication violates %s: %v", location, err)
		}
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestWorkerV1ValidateAndPublishUseSharedConsumerContract(t *testing.T) {
	h := newHarness(t)
	h.service.deps.Platform = domain.WorkerV1PlatformExecutionContract()
	session := h.profile.revisionSpec.Storage["session"]
	session.Destination.Username = deploymentv1.WorkerV1SessionRuntimeRole
	h.profile.revisionSpec.Storage["session"] = session
	h.profile.refresh(t)
	created := h.mustCreate(t)
	result, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "worker-v1-publication", Input: deploymentInput()})
	if err != nil {
		t.Fatal(err)
	}
	event, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(h.store.lastEvent.Payload)
	if err != nil {
		t.Fatal(err)
	}
	content, err := deploymentv1.VerifyRuntimeManifest(event.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = deploymentv1.ValidateWorkerV1(content, h.service.deps.Platform.Digest); err != nil {
		t.Fatal(err)
	}
	if result.Published.ManifestDigest != event.Manifest.ContentDigest {
		t.Fatal("published immutable identity differs")
	}
	// New contract does not mutate or reinterpret an existing publication receipt.
	first := append([]byte(nil), result.Published.Manifest.Content...)
	h.service.deps.Platform = domain.DefaultPlatformExecutionContract()
	replay, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "worker-v1-publication", Input: deploymentInput()})
	if err != nil || string(first) != string(replay.Published.Manifest.Content) {
		t.Fatalf("replay changed frozen publication: %v", err)
	}
}

func TestWorkerV1ValidateAndPublishRejectNonRuntimeSessionRole(t *testing.T) {
	h := newHarness(t) // The old platform-v1 fixture deliberately keeps username=agent.
	h.service.deps.Platform = domain.WorkerV1PlatformExecutionContract()
	created := h.mustCreate(t)
	report, err := h.service.ValidateDeploymentRevision(context.Background(), ValidateDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", Input: deploymentInput()})
	if err != nil {
		t.Fatal(err)
	}
	assertRoleDiagnostic := func(report domain.ValidationReport) {
		t.Helper()
		if report.Valid || len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != domain.DiagnosticStorageRoleUnsupported || report.Diagnostics[0].Path != "/resources/storage/session/destination/username" || !strings.Contains(report.Diagnostics[0].Message, "session runtime username must be session_runtime") {
			t.Fatalf("wrong role diagnostic: %#v", report)
		}
	}
	assertRoleDiagnostic(report)
	result, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "worker-v1-wrong-role", Input: deploymentInput()})
	if !errors.Is(err, ErrDeploymentRevisionInvalid) {
		t.Fatalf("Publish error: %v", err)
	}
	assertRoleDiagnostic(result.Validation)
	if len(h.store.published) != 0 || len(h.store.lastEvent.Payload) != 0 {
		t.Fatal("invalid role produced publication or event")
	}
}
