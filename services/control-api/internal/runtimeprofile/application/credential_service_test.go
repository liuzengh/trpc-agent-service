package application_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestSaveCredentialDraftEncryptsBeforePersisting(t *testing.T) {
	h := newCredentialHarness(t)
	secret := "first-private-model-value"
	command := modelCredentialCommand(1, "write-create", secret)
	result, err := h.service.SaveCredentialDraft(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProfileID != "rpf_a" || result.DraftRevision != 2 {
		t.Fatalf("unexpected write result: %#v", result)
	}
	spec := h.spec(t)
	id := spec.Models["primary"].APIKeyCredentialID
	if !strings.HasPrefix(id, "crd_") || len(h.store.records) != 1 {
		t.Fatal("save did not create exactly one opaque credential association")
	}
	record := h.store.records[id]
	if record.TenantID != "tnt_a" || record.ProfileID != "rpf_a" || record.Revision != 1 || record.Status != domain.CredentialActive {
		t.Fatal("credential ownership or initial version is incorrect")
	}
	if h.decrypt(t, record) != secret {
		t.Fatal("encrypted store did not preserve the supplied value")
	}
	if bytes.Contains(record.Ciphertext, []byte(secret)) || bytes.Contains(h.draft().Spec, []byte(secret)) {
		t.Fatal("plaintext escaped into stored ciphertext or canonical draft")
	}
	for _, receipt := range h.store.receipts {
		if bytes.Contains(receipt.Result, []byte(secret)) || bytes.Contains(receipt.Result, []byte(id)) || bytes.Contains(receipt.Result, []byte("configured")) {
			t.Fatal("receipt retained a value, internal association, or dynamic state")
		}
		if receipt.RequestMAC == "" || len(receipt.RequestMAC) != 64 {
			t.Fatal("receipt omitted the keyed request MAC")
		}
	}
	public, err := json.Marshal(result)
	if err != nil || bytes.Contains(public, []byte(secret)) || bytes.Contains(public, []byte(id)) {
		t.Fatal("write result disclosed credential material")
	}
	if *command.Write.Credentials["models"]["primary"]["api_key"].Value != secret {
		t.Fatal("save mutated caller-owned input")
	}
}

func TestSaveCredentialDraftCopyOnWriteKeepAndClear(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "create", "original-private-value"))
	oldID := h.spec(t).Models["primary"].APIKeyCredentialID
	oldRecord := h.store.records[oldID].Clone()
	h.save(t, modelCredentialCommand(2, "replace", "new-private-value"))
	newID := h.spec(t).Models["primary"].APIKeyCredentialID
	if oldID == newID || len(h.store.records) != 2 || !reflect.DeepEqual(oldRecord, h.store.records[oldID]) {
		t.Fatal("draft replacement modified the old credential instead of copy-on-write")
	}
	if h.decrypt(t, oldRecord) != "original-private-value" || h.decrypt(t, h.store.records[newID]) != "new-private-value" {
		t.Fatal("copy-on-write did not keep values independent")
	}
	keep := modelCredentialCommand(3, "keep", "unused")
	keep.Write.Credentials = nil
	keep.ActorUserID = "usr_member"
	h.save(t, keep)
	if h.spec(t).Models["primary"].APIKeyCredentialID != newID || len(h.store.records) != 2 {
		t.Fatal("missing credential field failed to keep the existing association")
	}
	clearCommand := modelCredentialCommand(4, "clear", "unused")
	clearCommand.Write.Credentials = credentialAction("models", "primary", "api_key", application.CredentialAction{Action: "clear"})
	h.save(t, clearCommand)
	if h.spec(t).Models["primary"].APIKeyCredentialID != "" {
		t.Fatal("draft clear retained its association")
	}
	if h.store.records[newID].Status != domain.CredentialActive || h.decrypt(t, h.store.records[newID]) != "new-private-value" {
		t.Fatal("draft clear unexpectedly revoked a live credential")
	}
}

func TestSaveCredentialDraftMemberCannotWriteOrDeleteCredentials(t *testing.T) {
	for _, operation := range []string{"replace", "clear", "delete"} {
		t.Run(operation, func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "original-private-value"))
			before := h.snapshot(t)
			command := modelCredentialCommand(2, "member-attempt", "replacement-private-value")
			command.ActorUserID = "usr_member"
			switch operation {
			case "clear":
				command.Write.Credentials = credentialAction("models", "primary", "api_key", application.CredentialAction{Action: "clear"})
			case "delete":
				command.Write.Config.Models = map[string]application.ModelConfig{}
				command.Write.Credentials = nil
			}
			_, err := h.service.SaveCredentialDraft(context.Background(), command)
			if !errors.Is(err, application.ErrTenantForbidden) {
				t.Fatalf("member %s: want tenant forbidden, got %v", operation, err)
			}
			if h.snapshot(t) != before {
				t.Fatal("forbidden credential mutation changed transaction state")
			}
		})
	}
}

func TestSaveCredentialDraftRejectsChangedEndpointWithKeep(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "private-original-value"))
	before := h.snapshot(t)
	command := modelCredentialCommand(2, "retarget", "unused")
	command.Write.Credentials = nil
	model := command.Write.Config.Models["primary"]
	model.BaseURL = "https://different.example.test/v1"
	command.Write.Config.Models["primary"] = model
	_, err := h.service.SaveCredentialDraft(context.Background(), command)
	if !errors.Is(err, domain.ErrCredentialAssociation) {
		t.Fatalf("keep silently retargeted a credential: %v", err)
	}
	if h.snapshot(t) != before {
		t.Fatal("failed purpose comparison changed state")
	}
}

func TestSaveCredentialDraftIdempotentReplayPrecedesMutableDraftCAS(t *testing.T) {
	h := newCredentialHarness(t)
	first := modelCredentialCommand(1, "stable-request", "private-first-value")
	original := h.save(t, first)
	h.now = h.now.Add(time.Hour)
	h.save(t, modelCredentialCommand(2, "another-write", "private-second-value"))
	before := h.snapshot(t)
	h.now = h.now.Add(time.Hour)
	replayed := h.save(t, first)
	if !reflect.DeepEqual(original, replayed) || h.snapshot(t) != before {
		t.Fatal("delayed idempotent replay changed the original result or state")
	}
	changed := modelCredentialCommand(1, "stable-request", "different-private-input")
	_, err := h.service.SaveCredentialDraft(context.Background(), changed)
	if !errors.Is(err, application.ErrCredentialIdempotencyConflict) {
		t.Fatalf("same idempotency key accepted different private input: %v", err)
	}
	if h.snapshot(t) != before {
		t.Fatal("conflicting idempotent request changed state")
	}
}

func TestSaveCredentialDraftReceiptReplayRechecksOwner(t *testing.T) {
	h := newCredentialHarness(t)
	command := modelCredentialCommand(1, "owner-request", "private-value")
	h.save(t, command)
	h.owners["tnt_a/usr_owner"] = false
	_, err := h.service.SaveCredentialDraft(context.Background(), command)
	if !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("former owner replay bypassed current authorization: %v", err)
	}
}

func TestSaveCredentialDraftFailureRollsBackWholeTransaction(t *testing.T) {
	for _, stage := range []string{"save", "receipt", "commit"} {
		t.Run(stage, func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "private-original-value"))
			before := h.snapshot(t)
			h.store.failAt = stage
			result, err := h.service.SaveCredentialDraft(context.Background(), modelCredentialCommand(2, "failing-write", "new-private-value"))
			if !errors.Is(err, errCredentialTestTransaction) || result != (application.DraftWriteResult{}) {
				t.Fatalf("%s failure did not return only transaction failure: %v", stage, err)
			}
			if h.snapshot(t) != before {
				t.Fatal("transaction failure left a credential, association, draft CAS, or receipt")
			}
		})
	}
}

func TestSaveCredentialDraftSeparatesDSNPasswordAndFixedDestination(t *testing.T) {
	h := newCredentialHarness(t)
	value := "postgresql://app:private-password@DB.EXAMPLE.TEST:5433/state?sslmode=verify-full"
	command := modelCredentialCommand(1, "dsn-create", "unused")
	command.Write.Config.Models = map[string]application.ModelConfig{}
	command.Write.Config.Storage["session"] = application.StorageConfig{Kind: domain.StorageKindPostgresState}
	command.Write.Credentials = credentialAction("storage", "session", "dsn", application.CredentialAction{Action: "replace", Value: &value})
	h.save(t, command)
	resource := h.spec(t).Storage["session"]
	want := domain.StorageDestination{Host: "db.example.test", Port: 5433, Database: "state", Username: "app", SSLMode: "verify-full"}
	if resource.Destination != want {
		t.Fatalf("DSN target not normalized and fixed: %#v", resource.Destination)
	}
	if h.decrypt(t, h.store.records[resource.DSNCredentialID]) != "private-password" {
		t.Fatal("encrypted DSN value was not separated from non-secret connection target")
	}
	if bytes.Contains(h.draft().Spec, []byte("private-password")) || bytes.Contains(h.draft().Spec, []byte("postgresql://")) {
		t.Fatal("original DSN escaped into persistent Spec")
	}
	before := h.snapshot(t)
	mismatch := command
	mismatch.IdempotencyKey = "dsn-retarget"
	mismatch.Write.ExpectedDraftRevision = 2
	other := want
	other.Host = "different.example.test"
	mismatch.Write.Config.Storage = map[string]application.StorageConfig{"session": {Kind: domain.StorageKindPostgresState, Destination: &other}}
	_, err := h.service.SaveCredentialDraft(context.Background(), mismatch)
	if !errors.Is(err, domain.ErrCredentialAssociation) || h.snapshot(t) != before {
		t.Fatalf("mismatching explicit destination was not atomically rejected: %v", err)
	}
}

func TestSaveCredentialDraftDeleteAndRecreateUsesNewIdentity(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "original", "old-private-value"))
	oldID := h.spec(t).Models["primary"].APIKeyCredentialID
	remove := modelCredentialCommand(2, "remove", "unused")
	remove.Write.Config.Models = map[string]application.ModelConfig{}
	remove.Write.Credentials = nil
	h.save(t, remove)
	if len(h.spec(t).Models) != 0 || h.store.records[oldID].Status != domain.CredentialActive {
		t.Fatal("full config replacement did not remove resource without revoking history")
	}
	h.save(t, modelCredentialCommand(3, "recreate", "new-private-value"))
	if h.spec(t).Models["primary"].APIKeyCredentialID == oldID {
		t.Fatal("same-name recreation reused the historical credential identity")
	}
}

func TestSaveCredentialDraftDoesNotKeepCrossScopeAssociation(t *testing.T) {
	for _, change := range []string{"tenant", "profile", "purpose"} {
		t.Run(change, func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "private-value"))
			id := h.spec(t).Models["primary"].APIKeyCredentialID
			record := h.store.records[id]
			switch change {
			case "tenant":
				record.TenantID = "tnt_b"
			case "profile":
				record.ProfileID = "rpf_b"
			case "purpose":
				record.Purpose = "bearer_token"
			}
			h.store.records[id] = record
			command := modelCredentialCommand(2, "keep-cross-scope", "unused")
			command.Write.Credentials = nil
			_, err := h.service.SaveCredentialDraft(context.Background(), command)
			if !errors.Is(err, domain.ErrCredentialAssociation) {
				t.Fatalf("kept credential from wrong %s: %v", change, err)
			}
		})
	}
}

func modelCredentialCommand(expected int64, key, value string) application.SaveCredentialDraftCommand {
	return application.SaveCredentialDraftCommand{
		TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", IdempotencyKey: key,
		Write: application.ProfileWrite{
			ExpectedDraftRevision: expected, CredentialProtocolVersion: domain.CredentialProtocolVersionV1,
			Config: application.ProfileConfig{
				Models: map[string]application.ModelConfig{"primary": {Kind: domain.ModelKindOpenAICompatible, Model: "example-model", BaseURL: "https://models.example.test/v1", Capabilities: []string{"chat"}}},
				Tools:  map[string]application.ToolConfig{}, Knowledge: map[string]application.KnowledgeConfig{}, Storage: map[string]application.StorageConfig{},
			},
			Credentials: credentialAction("models", "primary", "api_key", application.CredentialAction{Action: "replace", Value: &value}),
		},
	}
}

func credentialAction(category, name, purpose string, action application.CredentialAction) application.CredentialActions {
	return application.CredentialActions{category: {name: {purpose: action}}}
}

type credentialOwnerStub map[string]bool

func (owners credentialOwnerStub) IsActiveOwner(_ context.Context, tenant, actor string) (bool, error) {
	return owners[tenant+"/"+actor], nil
}

type credentialHarness struct {
	service *application.Service
	store   *credentialMemoryStore
	cipher  *credentialcrypto.Cipher
	owners  credentialOwnerStub
	now     time.Time
}

func newCredentialHarness(t *testing.T) *credentialHarness {
	return newCredentialHarnessWithBackend(t, nil)
}
func newCredentialHarnessWithBackend(t *testing.T, backend application.BackendAccess, targets ...application.ManagedCredentialTargetResolver) *credentialHarness {
	t.Helper()
	base := seededStore("tnt_a", "rpf_a")
	store := &credentialMemoryStore{base: base, records: map[string]domain.ProfileCredential{}, receipts: map[string]application.CredentialReceipt{}}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cipher, err := credentialcrypto.New(key)
	if err != nil {
		t.Fatal(err)
	}
	h := &credentialHarness{store: store, cipher: cipher, owners: credentialOwnerStub{"tnt_a/usr_owner": true}, now: testClock}
	var sequence int64
	var target application.ManagedCredentialTargetResolver
	if len(targets) > 0 {
		target = targets[0]
	}
	h.service = application.NewService(application.Dependencies{
		Store: base, Credentials: store, Cipher: cipher, OwnerAccess: h.owners, Backends: backend, ManagedCredentialTargets: target,
		TenantAccess:    accessStub{members: map[string]bool{"tnt_a/usr_owner": true, "tnt_a/usr_member": true}},
		NewProfileID:    func() (string, error) { return "rpf_unused", nil },
		NewRevisionID:   func() (string, error) { return "rpr_unused", nil },
		NewCredentialID: func() (string, error) { sequence++; return fmt.Sprintf("crd_%032x", sequence), nil },
		Now:             func() time.Time { return h.now },
	})
	return h
}

func (h *credentialHarness) save(t *testing.T, command application.SaveCredentialDraftCommand) application.DraftWriteResult {
	t.Helper()
	result, err := h.service.SaveCredentialDraft(context.Background(), command)
	if err != nil {
		t.Fatalf("save credential draft: %v", err)
	}
	return result
}
func (h *credentialHarness) draft() domain.ProfileDraft {
	return h.store.base.drafts[resourceKey("tnt_a", "rpf_a")].Clone()
}
func (h *credentialHarness) spec(t *testing.T) domain.Spec {
	t.Helper()
	var spec domain.Spec
	if err := json.Unmarshal(h.draft().Spec, &spec); err != nil {
		t.Fatal(err)
	}
	return spec
}
func (h *credentialHarness) decrypt(t *testing.T, record domain.ProfileCredential) string {
	t.Helper()
	value, err := h.cipher.Decrypt(context.Background(), record.AssociatedData(), record.Ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	return string(value)
}
func (h *credentialHarness) snapshot(t *testing.T) string {
	t.Helper()
	data, err := json.Marshal(struct {
		Draft    domain.ProfileDraft
		Records  map[string]domain.ProfileCredential
		Receipts map[string]application.CredentialReceipt
	}{h.draft(), h.store.records, h.store.receipts})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

var errCredentialTestTransaction = errors.New("injected credential transaction failure")

type credentialMemoryStore struct {
	mu       sync.Mutex
	base     *memoryStore
	records  map[string]domain.ProfileCredential
	receipts map[string]application.CredentialReceipt
	failAt   string
}

type credentialMemoryTransaction struct {
	store           *credentialMemoryStore
	tenant, profile string
	draft           domain.ProfileDraft
	records         map[string]domain.ProfileCredential
	receipts        map[string]application.CredentialReceipt
}

// This fake models the transaction port, not encryption. Every callback observes
// detached copies, and none of its changes become visible until commit succeeds.
func (store *credentialMemoryStore) WithinProfile(ctx context.Context, tenant, profile string, fn func(application.CredentialTransaction) error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.base.mu.Lock()
	defer store.base.mu.Unlock()
	key := resourceKey(tenant, profile)
	draft, ok := store.base.drafts[key]
	if !ok {
		return application.ErrRuntimeProfileNotFound
	}
	tx := &credentialMemoryTransaction{store: store, tenant: tenant, profile: profile, draft: draft.Clone(), records: map[string]domain.ProfileCredential{}, receipts: map[string]application.CredentialReceipt{}}
	for id, record := range store.records {
		tx.records[id] = record.Clone()
	}
	for id, receipt := range store.receipts {
		receipt.Result = bytes.Clone(receipt.Result)
		tx.receipts[id] = receipt
	}
	if err := fn(tx); err != nil {
		return err
	}
	if store.failAt == "commit" {
		return errCredentialTestTransaction
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.base.drafts[key] = tx.draft.Clone()
	store.records, store.receipts = tx.records, tx.receipts
	return nil
}
func (tx *credentialMemoryTransaction) GetDraft(context.Context) (domain.ProfileDraft, error) {
	return tx.draft.Clone(), nil
}
func (tx *credentialMemoryTransaction) SaveDraft(_ context.Context, draft domain.ProfileDraft, expected int64) error {
	if tx.store.failAt == "save" {
		return errCredentialTestTransaction
	}
	if draft.TenantID != tx.tenant || draft.ProfileID != tx.profile {
		return domain.ErrCredentialAssociation
	}
	if tx.draft.Revision != expected {
		return application.ErrDraftRevisionConflict
	}
	tx.draft = draft.Clone()
	return nil
}
func (tx *credentialMemoryTransaction) GetRevision(_ context.Context, number int64) (domain.ProfileRevision, error) {
	for _, revision := range tx.store.base.revisions[resourceKey(tx.tenant, tx.profile)] {
		if revision.RevisionNumber == number {
			return revision.Clone(), nil
		}
	}
	return domain.ProfileRevision{}, application.ErrProfileRevisionNotFound
}
func (tx *credentialMemoryTransaction) GetCredential(_ context.Context, id string) (domain.ProfileCredential, error) {
	record, ok := tx.records[id]
	if !ok {
		return domain.ProfileCredential{}, domain.ErrCredentialNotFound
	}
	return record.Clone(), nil
}
func (tx *credentialMemoryTransaction) InsertCredential(_ context.Context, record domain.ProfileCredential) error {
	if _, ok := tx.records[record.ID]; ok {
		return domain.ErrCredentialConflict
	}
	if record.TenantID != tx.tenant || record.ProfileID != tx.profile {
		return domain.ErrCredentialAssociation
	}
	tx.records[record.ID] = record.Clone()
	return nil
}
func (tx *credentialMemoryTransaction) UpdateCredential(_ context.Context, record domain.ProfileCredential, expected int64) error {
	current, ok := tx.records[record.ID]
	if !ok {
		return domain.ErrCredentialNotFound
	}
	if current.Revision != expected {
		return domain.ErrCredentialConflict
	}
	tx.records[record.ID] = record.Clone()
	return nil
}
func (tx *credentialMemoryTransaction) FindReceipt(_ context.Context, actor, key string) (application.CredentialReceipt, bool, error) {
	receipt, ok := tx.receipts[resourceKey(tx.tenant, tx.profile)+"/"+actor+"/"+key]
	receipt.Result = bytes.Clone(receipt.Result)
	return receipt, ok, nil
}
func (tx *credentialMemoryTransaction) InsertReceipt(_ context.Context, receipt application.CredentialReceipt) error {
	if tx.store.failAt == "receipt" {
		return errCredentialTestTransaction
	}
	key := resourceKey(tx.tenant, tx.profile) + "/" + receipt.ActorUserID + "/" + receipt.Key
	if _, ok := tx.receipts[key]; ok {
		return application.ErrCredentialIdempotencyConflict
	}
	receipt.Result = bytes.Clone(receipt.Result)
	tx.receipts[key] = receipt
	return nil
}

func TestSaveCredentialDraftConcurrentCASAllowsOnlyOneWinner(t *testing.T) {
	h := newCredentialHarness(t)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, key := range []string{"concurrent-one", "concurrent-two"} {
		go func(key string) {
			<-start
			_, err := h.service.SaveCredentialDraft(context.Background(), modelCredentialCommand(1, key, "private-value-for-"+key))
			results <- err
		}(key)
	}
	close(start)
	var succeeded, conflicted int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, application.ErrDraftRevisionConflict):
			conflicted++
		default:
			t.Fatalf("unexpected concurrent save result: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 || len(h.store.records) != 1 || len(h.store.receipts) != 1 || h.draft().Revision != 2 {
		t.Fatal("concurrent writes left more than one winning draft/credential/receipt")
	}
}
