package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestCreateUpdateAndReceiptReplayKeepOriginalRepresentation(t *testing.T) {
	h := newHarness(t)
	created, err := h.service.CreateDeployment(context.Background(), CreateDeploymentCommand{
		TenantID: "tenant-1", ActorUserID: "owner", IdempotencyKey: "create-1",
		Name: " Support ", Description: " first ",
	})
	if err != nil || !created.Created || created.Deployment.Name != "Support" || created.Deployment.MetadataRevision != 1 {
		t.Fatalf("CreateDeployment() = %#v, %v", created, err)
	}
	name := "Production"
	updated, err := h.service.UpdateDeploymentMetadata(context.Background(), UpdateDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.Deployment.ID, ActorUserID: "member",
		ExpectedMetadataRevision: 1, Name: &name,
	})
	if err != nil || updated.Name != "Production" || updated.MetadataRevision != 2 {
		t.Fatalf("UpdateDeploymentMetadata() = %#v, %v", updated, err)
	}
	replayed, err := h.service.CreateDeployment(context.Background(), CreateDeploymentCommand{
		TenantID: "tenant-1", ActorUserID: "member", IdempotencyKey: "create-1",
		Name: "Support", Description: "first",
	})
	if err != nil || replayed.Created || replayed.Deployment.Name != "Support" || replayed.Deployment.MetadataRevision != 1 {
		t.Fatalf("replayed CreateDeployment() = %#v, %v", replayed, err)
	}
	if _, err := h.service.CreateDeployment(context.Background(), CreateDeploymentCommand{
		TenantID: "tenant-1", ActorUserID: "owner", IdempotencyKey: "create-1",
		Name: "different",
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different replay error = %v", err)
	}
}

func TestCreateReceiptReplayRejectsUnknownFieldsAndImmutableOwnerDrift(t *testing.T) {
	t.Run("unknown receipt field", func(t *testing.T) {
		h := newHarness(t)
		created := h.mustCreate(t)
		key := receiptMapKey(CommandReceiptKey{
			TenantID: "tenant-1", Operation: "create", ScopeID: "tenant-1",
			KeyHash: keyHash("create-main"),
		})
		receipt := h.store.receipts[key]
		receipt.Result = append(receipt.Result[:len(receipt.Result)-1], []byte(`,"unknown":true}`)...)
		h.store.receipts[key] = receipt
		_, err := h.service.CreateDeployment(context.Background(), CreateDeploymentCommand{
			TenantID: "tenant-1", ActorUserID: "owner", IdempotencyKey: "create-main",
			Name: created.Name, Description: created.Description,
		})
		if !errors.Is(err, ErrPublicationIntegrity) {
			t.Fatalf("CreateDeployment() error = %v, want %v", err, ErrPublicationIntegrity)
		}
	})

	t.Run("created by drift", func(t *testing.T) {
		h := newHarness(t)
		created := h.mustCreate(t)
		current := h.store.deployments[created.ID]
		current.CreatedBy = "member"
		h.store.deployments[created.ID] = current
		_, err := h.service.CreateDeployment(context.Background(), CreateDeploymentCommand{
			TenantID: "tenant-1", ActorUserID: "owner", IdempotencyKey: "create-main",
			Name: created.Name, Description: created.Description,
		})
		if !errors.Is(err, ErrPublicationIntegrity) {
			t.Fatalf("CreateDeployment() error = %v, want %v", err, ErrPublicationIntegrity)
		}
	})
}

func TestMetadataCASRejectsStaleEvenForNoOp(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	name := created.Name
	if _, err := h.service.UpdateDeploymentMetadata(context.Background(), UpdateDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member",
		ExpectedMetadataRevision: 2, Name: &name,
	}); !errors.Is(err, ErrMetadataRevisionConflict) {
		t.Fatalf("stale no-op error = %v", err)
	}
	result, err := h.service.UpdateDeploymentMetadata(context.Background(), UpdateDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member",
		ExpectedMetadataRevision: 1, Name: &name,
	})
	if err != nil || result.MetadataRevision != 1 || h.store.updateCalls != 0 {
		t.Fatalf("current no-op = %#v, %v; writes=%d", result, err, h.store.updateCalls)
	}
}

func TestMetadataUpdateRejectsCrossRecordStoreResults(t *testing.T) {
	t.Run("query identity", func(t *testing.T) {
		h := newHarness(t)
		created := h.mustCreate(t)
		other := created
		other.TenantID = "tenant-other"
		h.store.getDeploymentOverride = &other
		name := "Updated"
		_, err := h.service.UpdateDeploymentMetadata(context.Background(), UpdateDeploymentCommand{
			TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member",
			ExpectedMetadataRevision: 1, Name: &name,
		})
		if !errors.Is(err, ErrPublicationIntegrity) {
			t.Fatalf("UpdateDeploymentMetadata() error = %v, want %v", err, ErrPublicationIntegrity)
		}
	})

	t.Run("write result", func(t *testing.T) {
		h := newHarness(t)
		created := h.mustCreate(t)
		other := created
		other.MetadataRevision = 2
		other.Name = "Other"
		other.UpdatedAt = other.UpdatedAt.Add(time.Second)
		h.store.updateResultOverride = &other
		name := "Updated"
		_, err := h.service.UpdateDeploymentMetadata(context.Background(), UpdateDeploymentCommand{
			TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member",
			ExpectedMetadataRevision: 1, Name: &name,
		})
		if !errors.Is(err, ErrPublicationIntegrity) {
			t.Fatalf("UpdateDeploymentMetadata() error = %v, want %v", err, ErrPublicationIntegrity)
		}
	})
}

func TestValidateAndPublishUseFixedSourcesAndProfileCredentialChecker(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	input := deploymentInput()
	report, err := h.service.ValidateDeploymentRevision(context.Background(), ValidateDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member", Input: input,
	})
	if err != nil || !report.Valid || h.checker.calls != 1 {
		t.Fatalf("ValidateDeploymentRevision() = %#v, %v calls=%d", report, err, h.checker.calls)
	}
	wantPurposes := []string{"api_key", "dsn"}
	gotPurposes := make([]string, 0, len(h.checker.last.Uses))
	for _, use := range h.checker.last.Uses {
		gotPurposes = append(gotPurposes, use.Purpose)
	}
	sort.Strings(gotPurposes)
	if fmt.Sprint(gotPurposes) != fmt.Sprint(wantPurposes) {
		t.Fatalf("credential purposes = %v, want %v", gotPurposes, wantPurposes)
	}
	published, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner",
		IdempotencyKey: "publish-1", Input: input,
	})
	if err != nil || !published.Created || !published.Validation.Valid ||
		published.Published.Revision.RevisionNumber != 1 || h.store.lastEvent.EventType != domain.ManifestPublishedType {
		t.Fatalf("PublishDeploymentRevision() = %#v, %v event=%#v", published, err, h.store.lastEvent)
	}
	if publicJSONContainsCredentialID(published.Published.ManifestView) {
		t.Fatalf("public view leaked credential identity: %s", published.Published.ManifestView)
	}
	beforeAgent, beforeProfile, beforeCheck := h.agent.calls, h.profile.calls, h.checker.calls
	h.checker.err = errors.New("backend is now offline")
	replayed, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner",
		IdempotencyKey: "publish-1", Input: input,
	})
	if err != nil || replayed.Created || replayed.Published.ManifestDigest != published.Published.ManifestDigest {
		t.Fatalf("publish replay = %#v, %v", replayed, err)
	}
	if h.agent.calls != beforeAgent || h.profile.calls != beforeProfile || h.checker.calls != beforeCheck {
		t.Fatalf("replay touched sources/checker: agent %d/%d profile %d/%d checker %d/%d",
			h.agent.calls, beforeAgent, h.profile.calls, beforeProfile, h.checker.calls, beforeCheck)
	}
}

func TestPublishRequiresOwnerAndCredentialFailuresAreStableDiagnostics(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	command := PublishDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member",
		IdempotencyKey: "publish-1", Input: deploymentInput(),
	}
	if _, err := h.service.PublishDeploymentRevision(context.Background(), command); !errors.Is(err, ErrTenantForbidden) {
		t.Fatalf("member publish error = %v", err)
	}
	command.ActorUserID = "owner"
	h.checker.err = profiledomain.ErrCredentialUnavailable
	result, err := h.service.PublishDeploymentRevision(context.Background(), command)
	if !errors.Is(err, ErrDeploymentRevisionInvalid) || result.Validation.Valid ||
		len(result.Validation.Diagnostics) != 1 || result.Validation.Diagnostics[0].Code != domain.DiagnosticCredentialUnavailable {
		t.Fatalf("credential failure = %#v, %v", result, err)
	}
	h.checker.err = errors.New("database unavailable")
	command.IdempotencyKey = "publish-2"
	if _, err := h.service.PublishDeploymentRevision(context.Background(), command); !errors.Is(err, ErrCredentialDependencyUnavailable) {
		t.Fatalf("credential dependency error = %v", err)
	}
}

func TestStaticValidationFailureSkipsCredentialChecker(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	delete(h.profile.revisionSpec.Models, "primary")
	h.profile.refresh(t)
	report, err := h.service.ValidateDeploymentRevision(context.Background(), ValidateDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member", Input: deploymentInput(),
	})
	if err != nil || report.Valid || h.checker.calls != 0 {
		t.Fatalf("static invalid report = %#v, %v checker calls=%d", report, err, h.checker.calls)
	}
}

type harness struct {
	service *Service
	store   *memoryStore
	agent   *agentReaderStub
	profile *profileReaderStub
	checker *credentialCheckerStub
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	store := &memoryStore{deployments: map[string]domain.Deployment{}, receipts: map[string]domain.CommandReceipt{}, published: map[string]domain.PublishedRevision{}}
	agent := newAgentReader(t)
	profile := newProfileReader(t)
	checker := &credentialCheckerStub{}
	now := time.Date(2026, time.September, 5, 3, 0, 0, 0, time.UTC)
	sequence := 0
	newID := func(prefix string) func() (string, error) {
		return func() (string, error) { sequence++; return fmt.Sprintf("%s_%d", prefix, sequence), nil }
	}
	service := NewService(Dependencies{
		Publications: store, Queries: store, TenantAccess: tenantAccessStub{},
		AgentVersions: agent, ProfileRevisions: profile, ProfileCredentials: checker,
		Platform: domain.DefaultPlatformExecutionContract(), NewDeploymentID: newID("dpl"),
		NewRevisionID: newID("dpr"), NewManifestID: newID("rmf"), NewEventID: newID("evt"),
		Now: func() time.Time { return now }, MaxEventBytes: 1024 * 1024,
	})
	return &harness{service: service, store: store, agent: agent, profile: profile, checker: checker}
}

func (h *harness) mustCreate(t *testing.T) domain.Deployment {
	t.Helper()
	result, err := h.service.CreateDeployment(context.Background(), CreateDeploymentCommand{
		TenantID: "tenant-1", ActorUserID: "owner", IdempotencyKey: "create-main", Name: "Main",
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Deployment
}

type tenantAccessStub struct{}

func (tenantAccessStub) IsActiveMember(_ context.Context, tenant, user string) (bool, error) {
	return tenant == "tenant-1" && (user == "owner" || user == "member"), nil
}
func (tenantAccessStub) IsActiveOwner(_ context.Context, tenant, user string) (bool, error) {
	return tenant == "tenant-1" && user == "owner", nil
}

type agentReaderStub struct {
	version agentdomain.AgentVersion
	calls   int
}

func newAgentReader(t *testing.T) *agentReaderStub {
	t.Helper()
	spec := agentdomain.Spec{
		SchemaVersion: "v1", Root: "assistant",
		Requirements: agentdomain.Requirements{
			Models: map[string]agentdomain.ModelRequirement{"primary": {Capabilities: []string{"chat"}}},
			Tools:  map[string]agentdomain.CapabilityRequirement{}, Knowledge: map[string]agentdomain.CapabilityRequirement{},
		},
		Nodes: map[string]agentdomain.Node{"assistant": {
			Kind: agentdomain.NodeKindLLM, Instruction: "Answer.", ModelSlot: "primary",
			ToolSlots: []string{}, KnowledgeSlots: []string{},
		}},
	}
	raw, _ := json.Marshal(spec)
	return &agentReaderStub{version: agentdomain.AgentVersion{
		ID: "agv-1", TenantID: "tenant-1", AgentID: "agent-1", VersionNumber: 1,
		SchemaVersion: "v1", Spec: raw, SpecDigest: "sha256:" + strings.Repeat("a", 64),
	}}
}
func (s *agentReaderStub) GetAgentVersion(_ context.Context, tenant, agent, _ string, number int64) (agentdomain.AgentVersion, error) {
	s.calls++
	if tenant != s.version.TenantID || agent != s.version.AgentID || number != s.version.VersionNumber {
		return agentdomain.AgentVersion{}, ErrAgentVersionNotFound
	}
	return s.version.Clone(), nil
}

type profileReaderStub struct {
	revision     profiledomain.ProfileRevision
	revisionSpec profiledomain.Spec
	calls        int
}

func newProfileReader(t *testing.T) *profileReaderStub {
	t.Helper()
	s := &profileReaderStub{revisionSpec: profiledomain.Spec{
		SchemaVersion: "v1", CredentialProtocolVersion: "v1",
		Models: map[string]profiledomain.ModelResource{"primary": {
			Kind: profiledomain.ModelKindOpenAICompatible, Model: "gpt-test",
			BaseURL: "https://model.example/v1", APIKeyCredentialID: "crd_00000000000000000000000000000001",
			Capabilities: []string{"chat"},
		}},
		Tools: map[string]profiledomain.ToolResource{}, Knowledge: map[string]profiledomain.KnowledgeResource{},
		Storage: map[string]profiledomain.StorageResource{"session": {
			Kind: profiledomain.StorageKindPostgresState, DSNCredentialID: "crd_00000000000000000000000000000002",
			Destination: profiledomain.StorageDestination{Host: "state.example", Port: 5432, Database: "state", Username: "agent", SSLMode: "verify-full"},
		}},
	}}
	s.refresh(t)
	return s
}
func (s *profileReaderStub) refresh(t *testing.T) {
	t.Helper()
	raw, _ := json.Marshal(s.revisionSpec)
	s.revision = profiledomain.ProfileRevision{ID: "rpr-1", TenantID: "tenant-1", ProfileID: "profile-1", RevisionNumber: 1, SchemaVersion: "v1", Spec: raw, SpecDigest: "sha256:" + strings.Repeat("b", 64)}
}
func (s *profileReaderStub) GetProfileRevision(_ context.Context, tenant, profile, _ string, number int64) (profiledomain.ProfileRevision, error) {
	s.calls++
	if tenant != s.revision.TenantID || profile != s.revision.ProfileID || number != s.revision.RevisionNumber {
		return profiledomain.ProfileRevision{}, ErrProfileRevisionNotFound
	}
	return s.revision.Clone(), nil
}

type credentialCheckerStub struct {
	calls int
	last  profileapp.CheckProfileCredentialsCommand
	err   error
}

func (s *credentialCheckerStub) CheckUsable(_ context.Context, command profileapp.CheckProfileCredentialsCommand) error {
	s.calls++
	s.last = command
	return s.err
}

func deploymentInput() domain.DeploymentInput {
	return domain.DeploymentInput{SchemaVersion: "v1", Agent: domain.AgentInput{AgentID: "agent-1", VersionNumber: 1}, Profile: domain.ProfileInput{ProfileID: "profile-1", RevisionNumber: 1}}
}

type memoryStore struct {
	deployments           map[string]domain.Deployment
	receipts              map[string]domain.CommandReceipt
	published             map[string]domain.PublishedRevision
	getDeploymentOverride *domain.Deployment
	updateResultOverride  *domain.Deployment
	updateCalls           int
	lastEvent             domain.OutboxEvent
}

func receiptMapKey(key CommandReceiptKey) string {
	return key.TenantID + "/" + key.Operation + "/" + key.ScopeID + "/" + key.KeyHash
}
func (s *memoryStore) FindCommandReceipt(_ context.Context, key CommandReceiptKey) (domain.CommandReceipt, bool, error) {
	v, ok := s.receipts[receiptMapKey(key)]
	return v, ok, nil
}
func (s *memoryStore) CreateDeployment(_ context.Context, commit CreateCommit) (CreateCommitResult, error) {
	key := receiptMapKey(CommandReceiptKey{TenantID: commit.Receipt.TenantID, Operation: commit.Receipt.Operation, ScopeID: commit.Receipt.ScopeID, KeyHash: commit.Receipt.KeyHash})
	if old, ok := s.receipts[key]; ok {
		return CreateCommitResult{Receipt: old}, nil
	}
	s.deployments[commit.Deployment.ID] = commit.Deployment
	s.receipts[key] = commit.Receipt
	return CreateCommitResult{Receipt: commit.Receipt, Created: true}, nil
}
func (s *memoryStore) UpdateDeploymentMetadata(_ context.Context, value domain.Deployment, expected int64) (domain.Deployment, error) {
	s.updateCalls++
	current, ok := s.deployments[value.ID]
	if !ok {
		return domain.Deployment{}, ErrDeploymentNotFound
	}
	if current.MetadataRevision != expected {
		return domain.Deployment{}, ErrMetadataRevisionConflict
	}
	s.deployments[value.ID] = value
	if s.updateResultOverride != nil {
		return *s.updateResultOverride, nil
	}
	return value, nil
}
func (s *memoryStore) CommitPublication(_ context.Context, commit PublicationCommit) (PublicationCommitResult, error) {
	key := receiptMapKey(CommandReceiptKey{TenantID: commit.Receipt.TenantID, Operation: commit.Receipt.Operation, ScopeID: commit.Receipt.ScopeID, KeyHash: commit.Receipt.KeyHash})
	if old, ok := s.receipts[key]; ok {
		return PublicationCommitResult{Receipt: old}, nil
	}
	d := s.deployments[commit.Revision.DeploymentID]
	if (d.LatestRevisionNumber == nil) != (commit.ExpectedLatestRevisionNumber == nil) || (d.LatestRevisionNumber != nil && *d.LatestRevisionNumber != *commit.ExpectedLatestRevisionNumber) {
		return PublicationCommitResult{}, ErrLatestRevisionConflict
	}
	n := commit.Revision.RevisionNumber
	d.LatestRevisionNumber = &n
	s.deployments[d.ID] = d
	view, _ := domain.PublicManifestViewFromJSON(commit.Manifest.Content)
	viewJSON, _ := json.Marshal(view)
	s.published[fmt.Sprintf("%s/%d", d.ID, n)] = domain.PublishedRevision{Revision: commit.Revision, Manifest: commit.Manifest, ManifestID: commit.Manifest.ID, ManifestDigest: commit.Manifest.ContentDigest, ManifestView: viewJSON}
	s.receipts[key] = commit.Receipt
	s.lastEvent = commit.OutboxEvent
	return PublicationCommitResult{Receipt: commit.Receipt, Created: true}, nil
}
func (s *memoryStore) GetDeployment(_ context.Context, tenant, id string) (domain.Deployment, error) {
	if s.getDeploymentOverride != nil {
		return *s.getDeploymentOverride, nil
	}
	v, ok := s.deployments[id]
	if !ok || v.TenantID != tenant {
		return domain.Deployment{}, ErrDeploymentNotFound
	}
	return v, nil
}
func (s *memoryStore) ListDeployments(_ context.Context, tenant string, _ Page) (DeploymentPage, error) {
	var out []domain.Deployment
	for _, v := range s.deployments {
		if v.TenantID == tenant {
			out = append(out, v)
		}
	}
	return DeploymentPage{Deployments: out, Total: len(out)}, nil
}
func (s *memoryStore) GetPublishedRevision(_ context.Context, tenant, id string, n int64) (domain.PublishedRevision, error) {
	v, ok := s.published[fmt.Sprintf("%s/%d", id, n)]
	if !ok || v.Revision.TenantID != tenant {
		return domain.PublishedRevision{}, ErrDeploymentRevisionNotFound
	}
	return v, nil
}
func (s *memoryStore) ListRevisionSummaries(_ context.Context, tenant, id string, _ Page) (RevisionSummaryPage, error) {
	if _, ok := s.deployments[id]; !ok {
		return RevisionSummaryPage{}, ErrDeploymentNotFound
	}
	var out []domain.DeploymentRevisionSummary
	for _, v := range s.published {
		if v.Revision.TenantID == tenant && v.Revision.DeploymentID == id {
			out = append(out, domain.DeploymentRevisionSummary{ID: v.Revision.ID, TenantID: tenant, DeploymentID: id, RevisionNumber: v.Revision.RevisionNumber, ManifestID: v.Manifest.ID, ManifestDigest: v.Manifest.ContentDigest})
		}
	}
	return RevisionSummaryPage{Revisions: out, Total: len(out)}, nil
}

func TestMetadataUpdateAllowsIndependentPublicationProgress(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	name := "Updated"
	next := created
	if _, err := next.UpdateMetadata(&name, nil, h.service.deps.Now()); err != nil {
		t.Fatal(err)
	}
	latest := int64(1)
	next.LatestRevisionNumber = &latest
	h.store.updateResultOverride = &next
	result, err := h.service.UpdateDeploymentMetadata(context.Background(), UpdateDeploymentCommand{
		TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "member",
		ExpectedMetadataRevision: 1, Name: &name,
	})
	if err != nil || result.MetadataRevision != 2 || result.LatestRevisionNumber == nil || *result.LatestRevisionNumber != 1 {
		t.Fatalf("metadata update with independent publication = %#v, %v", result, err)
	}
}

func TestPublicationEventSizeBoundaries(t *testing.T) {
	probe := newHarness(t)
	created := probe.mustCreate(t)
	command := PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "size", Input: deploymentInput()}
	if _, err := probe.service.PublishDeploymentRevision(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	size := len(probe.store.lastEvent.Payload)
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("limit_delta_%d", delta), func(t *testing.T) {
			h := newHarness(t)
			created := h.mustCreate(t)
			command.DeploymentID = created.ID
			h.service.deps.MaxEventBytes = size + delta
			result, err := h.service.PublishDeploymentRevision(context.Background(), command)
			if delta < 0 {
				if !errors.Is(err, ErrDeploymentRevisionInvalid) || result.Validation.Valid || len(h.store.published) != 0 || h.store.lastEvent.ID != "" {
					t.Fatalf("oversize publication = %#v, %v", result, err)
				}
			} else if err != nil || !result.Created || len(h.store.lastEvent.Payload) != size {
				t.Fatalf("within-limit publication = %#v, %v size=%d", result, err, len(h.store.lastEvent.Payload))
			}
		})
	}
}

func TestServiceCapturesOwnedPlatformContractSnapshot(t *testing.T) {
	h := newHarness(t)
	deps := h.service.deps
	service := NewService(deps)
	expected := service.deps.Platform.Digest
	deps.Platform.Execution.MaxRunSeconds = 1
	delete(deps.Platform.ModelAdapters, profiledomain.ModelKindOpenAICompatible)
	if err := service.deps.Platform.Validate(); err != nil || service.deps.Platform.Digest != expected {
		t.Fatalf("caller mutation changed captured platform contract: %v", err)
	}
}
