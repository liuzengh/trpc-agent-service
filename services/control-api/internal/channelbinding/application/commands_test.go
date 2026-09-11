package application

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type memoryState struct {
	accounts map[string]Aggregate
	receipts map[ReceiptKey]Receipt
	catalog  int64
	events   []domain.RouteProjection
}
type memoryStore struct {
	mu          sync.Mutex
	data        memoryState
	owner       bool
	failReceipt bool
	beforeTx    func()
}
type memoryTx struct {
	store *memoryStore
	scope WriteScope
	data  memoryState
}

func cloneAggregate(a Aggregate) Aggregate {
	a.Credentials = append([]domain.CredentialRecord(nil), a.Credentials...)
	for i := range a.Credentials {
		a.Credentials[i].Ciphertext = bytes.Clone(a.Credentials[i].Ciphertext)
	}
	if a.Binding != nil {
		v := *a.Binding
		if v.Traffic != nil {
			traffic := *v.Traffic
			traffic.CanarySubjects = slices.Clone(traffic.CanarySubjects)
			v.Traffic = &traffic
		}
		a.Binding = &v
	}
	if a.Route.Projection != nil {
		v := *a.Route.Projection
		if v.Route.Traffic != nil {
			traffic := *v.Route.Traffic
			traffic.CanarySubjects = slices.Clone(traffic.CanarySubjects)
			v.Route.Traffic = &traffic
		}
		a.Route.Projection = &v
	}
	return a
}
func cloneState(s memoryState) memoryState {
	out := memoryState{accounts: map[string]Aggregate{}, receipts: map[ReceiptKey]Receipt{}, catalog: s.catalog, events: append([]domain.RouteProjection{}, s.events...)}
	for k, a := range s.accounts {
		out.accounts[k] = cloneAggregate(a)
	}
	for k, r := range s.receipts {
		r.Result = bytes.Clone(r.Result)
		out.receipts[k] = r
	}
	return out
}
func (m *memoryStore) FindReceipt(_ context.Context, k ReceiptKey) (Receipt, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.data.receipts[k]
	return r, ok, nil
}
func (m *memoryStore) WithWrite(ctx context.Context, scope WriteScope, fn func(Transaction) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.beforeTx != nil {
		m.beforeTx()
		m.beforeTx = nil
	}
	if !m.owner {
		return ErrPermissionDenied
	}
	tx := &memoryTx{m, scope, cloneState(m.data)}
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.data = tx.data
	return nil
}
func (t *memoryTx) FindReceipt(_ context.Context, k ReceiptKey) (Receipt, bool, error) {
	r, ok := t.data.receipts[k]
	return r, ok, nil
}
func (t *memoryTx) LoadAccount(_ context.Context, id string) (Aggregate, error) {
	a, ok := t.data.accounts[id]
	if !ok || a.Account.TenantID != t.scope.Actor.TenantID {
		return Aggregate{}, ErrAccountNotFound
	}
	return cloneAggregate(a), nil
}
func (t *memoryTx) LoadBinding(ctx context.Context, id string) (Aggregate, error) {
	for _, a := range t.data.accounts {
		if a.Binding != nil && a.Binding.ID == id && a.Account.TenantID == t.scope.Actor.TenantID {
			return t.LoadAccount(ctx, a.Account.ID)
		}
	}
	return Aggregate{}, ErrBindingNotFound
}
func (t *memoryTx) Save(_ context.Context, a Aggregate) error {
	if a.Account.TenantID != t.scope.Actor.TenantID || a.Account.ScopeID != t.scope.ScopeID {
		return ErrPermissionDenied
	}
	if err := a.Account.Validate(); err != nil {
		return err
	}
	old, exists := t.data.accounts[a.Account.ID]
	for id, other := range t.data.accounts {
		if id != a.Account.ID && other.Account.Provider == a.Account.Provider && other.Account.ProviderAccountID == a.Account.ProviderAccountID {
			return ErrIdentityConflict
		}
	}
	if !exists || old.Account != a.Account {
		t.data.catalog++
	}
	if a.Route.Generation > old.Route.Generation {
		t.data.events = append(t.data.events, *a.Route.Projection)
	}
	t.data.accounts[a.Account.ID] = cloneAggregate(a)
	return nil
}
func (t *memoryTx) SaveReceipt(_ context.Context, r Receipt) error {
	if t.store.failReceipt {
		return errors.New("injected receipt failure")
	}
	if _, ok := t.data.receipts[r.Key]; ok {
		return errors.New("duplicate receipt")
	}
	t.data.receipts[r.Key] = r
	return nil
}
func (m *memoryStore) GetAccount(_ context.Context, tenant, id string) (Aggregate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.data.accounts[id]
	if !ok || a.Account.TenantID != tenant {
		return Aggregate{}, ErrAccountNotFound
	}
	return cloneAggregate(a), nil
}
func (m *memoryStore) GetBinding(_ context.Context, tenant, id string) (Aggregate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.data.accounts {
		if a.Binding != nil && a.Binding.ID == id && a.Account.TenantID == tenant {
			return cloneAggregate(a), nil
		}
	}
	return Aggregate{}, ErrBindingNotFound
}

type access struct {
	allow bool
	owner bool
}

func (a *access) IsActiveMember(context.Context, string, string) (bool, error) { return a.allow, nil }
func (a *access) IsActiveOwner(context.Context, string, string) (bool, error)  { return a.owner, nil }

type targets struct {
	reads atomic.Int64
	fail  atomic.Bool
}

func (t *targets) ReadExact(_ context.Context, tenant, user string, s domain.TargetSelector) (domain.PublishedTarget, error) {
	t.reads.Add(1)
	if t.fail.Load() {
		return domain.PublishedTarget{}, ErrDependencyUnavailable
	}
	return domain.PublishedTarget{TenantID: tenant, DeploymentID: s.DeploymentID, RevisionNumber: s.RevisionNumber, DeploymentRevisionID: "dpr_" + s.DeploymentID, ManifestID: "rmf_" + s.DeploymentID, ManifestDigest: "sha256:" + strings.Repeat("a", 64)}, nil
}
func setup(t *testing.T) (*Service, *memoryStore, *access, *targets) {
	t.Helper()
	m := &memoryStore{data: memoryState{accounts: map[string]Aggregate{}, receipts: map[ReceiptKey]Receipt{}, catalog: 1}, owner: true}
	a := &access{true, true}
	r := &targets{}
	cipher, err := credentialcrypto.New("k1", map[string]credentialcrypto.Key{"k1": {Encryption: bytes.Repeat([]byte{1}, 32), MAC: bytes.Repeat([]byte{2}, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	var seq atomic.Int64
	s, err := NewService(Dependencies{Commands: m, Queries: m, TenantAccess: a, Targets: r, Cipher: cipher, ScopeID: "gateway_pool", NewID: func(prefix string) (string, error) { return fmt.Sprintf("%s_%d", prefix, seq.Add(1)), nil }, Now: func() time.Time { return time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	return s, m, a, r
}
func input() CreateAccountInput {
	token, secret := "TEST_ONLY_BOT_TOKEN", "TEST_ONLY_WEBHOOK_SECRET"
	return CreateAccountInput{Provider: domain.Telegram, ProviderAccountID: "00123", Name: " test ", Credentials: map[string]domain.CredentialEdit{domain.TelegramBotToken: {Action: "replace", Value: &token}, domain.TelegramWebhookSecret: {Action: "replace", Value: &secret}}}
}

var owner = Actor{"tnt_a", "usr_owner"}

func create(t *testing.T, s *Service) CommandResult {
	t.Helper()
	r, err := s.CreateAccount(context.Background(), owner, "create", input())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestCreateEncryptsAndReplaysNormalizedCommand(t *testing.T) {
	s, m, _, _ := setup(t)
	first := create(t, s)
	if first.Account.Enabled || first.Account.Revision != 1 || m.data.catalog != 2 || len(m.data.events) != 0 {
		t.Fatal("initial state")
	}
	i := input()
	i.Name = "test"
	i.ProviderAccountID = "123"
	again, err := s.CreateAccount(context.Background(), owner, "create", i)
	if err != nil || !reflect.DeepEqual(first, again) || len(m.data.accounts) != 1 {
		t.Fatal("replay", err)
	}
	i.Credentials[domain.TelegramBotToken] = domain.CredentialEdit{Action: "replace", Value: ptr("DIFFERENT_TEST_VALUE")}
	_, err = s.CreateAccount(context.Background(), owner, "create", i)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("different credential did not conflict", err)
	}
	stored := m.data.accounts[first.Account.ID]
	for _, c := range stored.Credentials {
		if bytes.Contains(c.Ciphertext, []byte("TEST_ONLY")) {
			t.Fatal("plaintext persisted")
		}
		aad, _ := c.AAD()
		value, err := s.deps.Cipher.Decrypt(context.Background(), c.KeyID, aad, c.Ciphertext)
		if err != nil || !bytes.HasPrefix(value, []byte("TEST_ONLY")) {
			t.Fatal("ciphertext binding", err)
		}
	}
	for _, r := range m.data.receipts {
		if bytes.Contains(r.Result, []byte("TEST_ONLY")) || bytes.Contains(r.Result, []byte("credential_id")) || len(r.RequestMAC) != 64 {
			t.Fatal("receipt carries secret or lacks MAC")
		}
	}
}
func ptr(v string) *string { return &v }
func TestCreateRollbackAndTransactionAuthorization(t *testing.T) {
	s, m, a, _ := setup(t)
	m.failReceipt = true
	_, err := s.CreateAccount(context.Background(), owner, "create", input())
	if err == nil || len(m.data.accounts) != 0 || m.data.catalog != 1 || len(m.data.receipts) != 0 {
		t.Fatal("partial commit survived")
	}
	m.failReceipt = false
	a.owner = false
	_, err = s.CreateAccount(context.Background(), owner, "create", input())
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatal(err)
	}
	a.owner = true
	m.beforeTx = func() { m.owner = false }
	_, err = s.CreateAccount(context.Background(), owner, "create", input())
	if !errors.Is(err, ErrPermissionDenied) || len(m.data.accounts) != 0 {
		t.Fatal("revoked transaction owner accepted", err)
	}
}
func TestBindingLifecycleUsesExactTargetAndIndependentVersions(t *testing.T) {
	s, m, _, r := setup(t)
	ctx := context.Background()
	created := create(t, s)
	id := created.Account.ID
	enabled, err := s.SetAccountEnabled(ctx, owner, id, "account-on", AccountEnabledInput{1, true})
	if err != nil || enabled.Account.ConnectionRevision != 2 {
		t.Fatal(err)
	}
	binding, err := s.CreateBinding(ctx, owner, "bind", CreateBindingInput{id, domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 1}})
	if err != nil || binding.Binding.Enabled || binding.RouteGeneration != 1 || binding.EventID == "" {
		t.Fatal("create binding", err)
	}
	beforeReads := r.reads.Load()
	r.fail.Store(true)
	retry, err := s.CreateBinding(ctx, owner, "bind", CreateBindingInput{id, domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 1}})
	if err != nil || !reflect.DeepEqual(retry, binding) || r.reads.Load() != beforeReads {
		t.Fatal("receipt replay re-read target", err)
	}
	r.fail.Store(false)
	on, err := s.SetBindingEnabled(ctx, owner, binding.Binding.ID, "binding-on", BindingEnabledInput{1, true})
	if err != nil || on.RouteGeneration != 2 || on.Account.ConnectionRevision != 2 {
		t.Fatal(err)
	}
	off, err := s.SetAccountEnabled(ctx, owner, id, "account-off", AccountEnabledInput{2, false})
	if err != nil || off.RouteGeneration != 3 || !off.Binding.Enabled {
		t.Fatal(err)
	}
	moved, err := s.SetBindingTarget(ctx, owner, binding.Binding.ID, "move", BindingTargetInput{2, domain.TargetSelector{DeploymentID: "dpl_b", RevisionNumber: 1}})
	if err != nil || moved.EventID != "" || moved.RouteGeneration != 3 || moved.Binding.Revision != 3 {
		t.Fatal("disabled move should not emit", err)
	}
	restored, err := s.SetAccountEnabled(ctx, owner, id, "restore", AccountEnabledInput{3, true})
	if err != nil || restored.RouteGeneration != 4 || restored.Account.MinRouteGeneration != 4 || restored.Account.ConnectionRevision != 4 {
		t.Fatal(err)
	}
	if len(m.data.events) != 4 || m.data.events[1].Route.ManifestRef != "rmf_dpl_a" || m.data.events[3].Route.ManifestRef != "rmf_dpl_b" {
		t.Fatal("historical route snapshot changed")
	}
	for _, event := range m.data.events {
		raw, _, err := event.Encode()
		if err != nil || bytes.Contains(raw, []byte("TEST_ONLY")) {
			t.Fatal("route wire", err)
		}
	}
}

func TestBindingTrafficPublishesCompletePolicyAndReplays(t *testing.T) {
	s, m, _, targets := setup(t)
	ctx := context.Background()
	created := create(t, s)
	accountID := created.Account.ID
	if _, err := s.SetAccountEnabled(ctx, owner, accountID, "account-on", AccountEnabledInput{1, true}); err != nil {
		t.Fatal(err)
	}
	binding, err := s.CreateBinding(ctx, owner, "bind", CreateBindingInput{accountID, domain.TargetSelector{DeploymentID: "dpl_stable", RevisionNumber: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetBindingEnabled(ctx, owner, binding.Binding.ID, "binding-on", BindingEnabledInput{1, true}); err != nil {
		t.Fatal(err)
	}

	beforeReads := targets.reads.Load()
	input := BindingTrafficInput{
		ExpectedBindingRevision: 2,
		Target:                  domain.TargetSelector{DeploymentID: "dpl_canary", RevisionNumber: 2},
		PercentageBasisPoints:   1500,
		CanarySubjects:          []string{"usr_b", "usr_a"},
	}
	rollout, err := s.SetBindingTraffic(ctx, owner, binding.Binding.ID, "traffic", input)
	if err != nil || rollout.Binding.Traffic == nil || rollout.Binding.Revision != 3 || rollout.RouteGeneration != 3 || rollout.EventID == "" {
		t.Fatalf("set traffic: %+v err=%v", rollout, err)
	}
	traffic := m.data.events[len(m.data.events)-1].Route.Traffic
	if traffic == nil || traffic.Target.DeploymentRevisionID != "dpr_dpl_canary" || traffic.PercentageBasisPoints != 1500 || !slices.Equal(traffic.CanarySubjects, []string{"usr_a", "usr_b"}) {
		t.Fatalf("incomplete route traffic: %+v", traffic)
	}

	readsAfterFirst := targets.reads.Load()
	if readsAfterFirst != beforeReads+1 {
		t.Fatalf("candidate read count=%d want=%d", readsAfterFirst, beforeReads+1)
	}
	replay, err := s.SetBindingTraffic(ctx, owner, binding.Binding.ID, "traffic", input)
	if err != nil || !reflect.DeepEqual(replay, rollout) || targets.reads.Load() != readsAfterFirst {
		t.Fatalf("receipt replay re-read candidate: %+v err=%v", replay, err)
	}

	// Changing only the percentage retains rollout identity and emits a new
	// complete route generation.
	input.ExpectedBindingRevision = 3
	input.PercentageBasisPoints = 3000
	updated, err := s.SetBindingTraffic(ctx, owner, binding.Binding.ID, "traffic-expand", input)
	if err != nil || updated.Binding.Traffic.ID != rollout.Binding.Traffic.ID || updated.RouteGeneration != 4 {
		t.Fatalf("expand traffic: %+v err=%v", updated, err)
	}
	if _, err = s.SetAccountEnabled(ctx, owner, accountID, "account-off", AccountEnabledInput{2, false}); err != nil {
		t.Fatal(err)
	}
	beforeRestore := targets.reads.Load()
	restored, err := s.SetAccountEnabled(ctx, owner, accountID, "account-restore", AccountEnabledInput{3, true})
	if err != nil || restored.RouteGeneration != 6 || restored.Binding.Traffic == nil || targets.reads.Load() != beforeRestore+2 {
		t.Fatalf("account restore did not revalidate rollout: %+v reads=%d before=%d err=%v", restored, targets.reads.Load(), beforeRestore, err)
	}
}

func TestEnablingBindingRevalidatesStableAndCanaryTargets(t *testing.T) {
	s, _, _, targets := setup(t)
	ctx := context.Background()
	created := create(t, s)
	binding, err := s.CreateBinding(ctx, owner, "bind", CreateBindingInput{created.Account.ID, domain.TargetSelector{DeploymentID: "dpl_stable", RevisionNumber: 1}})
	if err != nil {
		t.Fatal(err)
	}
	traffic, err := s.SetBindingTraffic(ctx, owner, binding.Binding.ID, "traffic", BindingTrafficInput{
		ExpectedBindingRevision: 1,
		Target:                  domain.TargetSelector{DeploymentID: "dpl_canary", RevisionNumber: 1},
		PercentageBasisPoints:   500,
		CanarySubjects:          []string{},
	})
	if err != nil || traffic.RouteGeneration != 1 || traffic.EventID != "" {
		t.Fatalf("disabled policy update: %+v err=%v", traffic, err)
	}
	if _, err = s.SetAccountEnabled(ctx, owner, created.Account.ID, "account-on", AccountEnabledInput{1, true}); err != nil {
		t.Fatal(err)
	}
	before := targets.reads.Load()
	enabled, err := s.SetBindingEnabled(ctx, owner, binding.Binding.ID, "binding-on", BindingEnabledInput{2, true})
	if err != nil || enabled.RouteGeneration != 2 || targets.reads.Load() != before+2 {
		t.Fatalf("enable did not revalidate both targets: %+v reads=%d before=%d err=%v", enabled, targets.reads.Load(), before, err)
	}
}
func TestCredentialRotationCASAndReceiptReplay(t *testing.T) {
	s, m, _, _ := setup(t)
	ctx := context.Background()
	created := create(t, s)
	id := created.Account.ID
	change := UpdateCredentialInput{ExpectedAccountRevision: 1, ExpectedCredentialVersion: 1, CredentialEdit: domain.CredentialEdit{Action: "replace", Value: ptr("NEW_TEST_ONLY_VALUE")}}
	first, err := s.UpdateCredential(ctx, owner, id, domain.TelegramBotToken, "rotate", change)
	if err != nil || first.Account.Revision != 2 || first.Account.ConnectionRevision != 2 {
		t.Fatal(err)
	}
	again, err := s.UpdateCredential(ctx, owner, id, domain.TelegramBotToken, "rotate", change)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("rotation replay", err)
	}
	_, err = s.UpdateCredential(ctx, owner, id, domain.TelegramBotToken, "rotate-again", change)
	var e *domain.Error
	if !errors.As(err, &e) || e.Code != domain.RevisionConflict {
		t.Fatal("old CAS accepted", err)
	}
	if m.data.catalog != 3 {
		t.Fatal("receipt replay advanced catalog")
	}
}
func TestConcurrentSameKeyAndConflictingCAS(t *testing.T) {
	s, m, _, _ := setup(t)
	ctx := context.Background()
	const count = 12
	var wg sync.WaitGroup
	errs := make(chan error, count)
	ids := make(chan string, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.CreateAccount(ctx, owner, "same", input())
			errs <- err
			if err == nil {
				ids <- r.Account.ID
			}
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id string
	for v := range ids {
		if id != "" && id != v {
			t.Fatal("different receipt identity")
		}
		id = v
	}
	if len(m.data.accounts) != 1 || m.data.catalog != 2 {
		t.Fatal("duplicate business commit")
	}
	var wins atomic.Int64
	errs = make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("name%d", i)
			_, err := s.UpdateAccount(ctx, owner, id, fmt.Sprintf("edit%d", i), UpdateAccountInput{ExpectedAccountRevision: 1, Name: &name})
			if err == nil {
				wins.Add(1)
			} else {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	if wins.Load() != 1 {
		t.Fatalf("CAS winners=%d", wins.Load())
	}
	for err := range errs {
		var e *domain.Error
		if !errors.As(err, &e) || e.Code != domain.RevisionConflict {
			t.Fatal(err)
		}
	}
}
func TestCrossTenantAndFailedReceiptPreservePublishedRoute(t *testing.T) {
	s, m, _, _ := setup(t)
	ctx := context.Background()
	created := create(t, s)
	_, err := s.UpdateAccount(ctx, Actor{"tnt_b", "usr_owner"}, created.Account.ID, "cross", UpdateAccountInput{ExpectedAccountRevision: 1, Name: ptr("x")})
	if !errors.Is(err, ErrAccountNotFound) {
		t.Fatal(err)
	}
	binding, err := s.CreateBinding(ctx, owner, "bind", CreateBindingInput{created.Account.ID, domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 1}})
	if err != nil {
		t.Fatal(err)
	}
	before := cloneState(m.data)
	m.failReceipt = true
	_, err = s.SetAccountEnabled(ctx, owner, created.Account.ID, "enable", AccountEnabledInput{1, true})
	if err == nil || !reflect.DeepEqual(before, m.data) {
		t.Fatal("failed command left partial account/route state")
	}
	raw, _ := json.Marshal(binding)
	if strings.Contains(string(raw), "TEST_ONLY") {
		t.Fatal("public result leaked value")
	}
}
