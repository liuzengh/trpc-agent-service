package application

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// This transaction fake controls database time and verifies rollback semantics;
// locking, SQL constraints and durable races are covered by real PG tests.
type preflightMemory struct {
	mu                         sync.Mutex
	now                        time.Time
	epoch                      string
	accounts                   *memoryStore
	records                    map[string]PreflightRecord
	receipts                   map[string]PreflightRequest
	requester, session, tenant bool
	failReceipt                bool
	counts                     *[3]int
}

func clonePF[T any](v T) T {
	raw, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(raw, &out)
	return out
}
func (m *preflightMemory) WithTransaction(ctx context.Context, _ string, fn func(PreflightTransaction) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldRecords, oldReceipts := clonePF(m.records), clonePF(m.receipts)
	err := fn(m)
	if err != nil {
		m.records, m.receipts = oldRecords, oldReceipts
	}
	return err
}
func (m *preflightMemory) Now(context.Context) (time.Time, error) { return m.now, nil }
func (m *preflightMemory) SourceEpoch() string                    { return m.epoch }
func (m *preflightMemory) LockAccount(ctx context.Context, tenant, account, user string, _ bool, additional ...string) (PreflightAccount, error) {
	a, err := m.accounts.GetAccount(ctx, tenant, account)
	owners := map[string]bool{user: m.requester}
	for _, id := range additional {
		owners[id] = m.requester
	}
	return PreflightAccount{Account: a.Account, Credentials: a.Credentials, TenantActive: m.tenant, RequesterOwner: m.requester, SessionOwner: m.session, RequesterOwners: owners}, err
}
func (m *preflightMemory) Load(_ context.Context, id string) (PreflightRecord, bool, error) {
	r, ok := m.records[id]
	return clonePF(r), ok, nil
}
func (m *preflightMemory) Save(_ context.Context, r PreflightRecord) error {
	if err := preflightWire("preflight-view.schema.json", r.View); err != nil {
		return err
	}
	if old, ok := m.records[r.View.PreflightID]; ok && !preflightActive(old) && !reflect.DeepEqual(old, r) {
		return fmt.Errorf("terminal fact mutated")
	}
	m.records[r.View.PreflightID] = clonePF(r)
	return nil
}
func (m *preflightMemory) FindRequest(_ context.Context, kind, key string) (PreflightRequest, bool, error) {
	r, ok := m.receipts[kind+key]
	return clonePF(r), ok, nil
}
func (m *preflightMemory) SaveRequest(_ context.Context, r PreflightRequest) error {
	if m.failReceipt {
		return ErrDependencyUnavailable
	}
	m.receipts[r.Kind+r.Key] = clonePF(r)
	return nil
}
func (m *preflightMemory) Active(_ context.Context, tenant, account string) ([]PreflightRecord, error) {
	var out []PreflightRecord
	for _, r := range m.records {
		if r.View.TenantID == tenant && r.View.AccountID == account && preflightActive(r) {
			out = append(out, clonePF(r))
		}
	}
	return out, nil
}
func (m *preflightMemory) Counts(_ context.Context, tenant, account string, since time.Time) (int, int, int, error) {
	if m.counts != nil {
		return m.counts[0], m.counts[1], m.counts[2], nil
	}
	ac, tc, ta := 0, 0, 0
	for _, r := range m.records {
		if r.View.TenantID == tenant {
			if preflightActive(r) {
				ta++
			}
			if r.View.RequestedAt.After(since) {
				tc++
				if r.View.AccountID == account {
					ac++
				}
			}
		}
	}
	return ac, tc, ta, nil
}
func (m *preflightMemory) ClaimCount(_ context.Context, principal, instance string, since time.Time) (int, error) {
	n := 0
	for _, r := range m.receipts {
		if r.Kind == "claim" && r.PrincipalID == principal && r.InstanceID == instance && r.CreatedAt.After(since) {
			n++
		}
	}
	return n, nil
}
func (m *preflightMemory) Lookup(_ context.Context, scope, id string) (PreflightKey, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[id]
	return PreflightKey{id, r.View.TenantID, r.View.AccountID, r.View.RequestedBy}, ok && r.ScopeID == scope, nil
}
func (m *preflightMemory) Candidate(_ context.Context, scope string, policy ...string) (PreflightKey, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	diagnosticPolicy := ""
	if len(policy) == 1 {
		diagnosticPolicy = policy[0]
	}
	for id, r := range m.records {
		if r.View.DiagnosticPolicy == diagnosticPolicy && r.ScopeID == scope && preflightActive(r) && (r.View.State == "QUEUED" || !m.now.Before(*r.LeaseExpiresAt)) {
			return PreflightKey{id, r.View.TenantID, r.View.AccountID, r.View.RequestedBy}, true, nil
		}
	}
	return PreflightKey{}, false, nil
}
func (m *preflightMemory) MaintenanceCandidates(_ context.Context, scope string, _ int) ([]PreflightKey, error) {
	var out []PreflightKey
	for id, r := range m.records {
		if r.ScopeID == scope && preflightActive(r) {
			out = append(out, PreflightKey{id, r.View.TenantID, r.View.AccountID, r.View.RequestedBy})
		}
	}
	return out, nil
}
func (*preflightMemory) Cleanup(context.Context) error { return nil }
func preflightSetup(t *testing.T) (*PreflightService, *preflightMemory, string, channelv1.PreflightCreateRequest) {
	t.Helper()
	base, accounts, access, _ := setup(t)
	in := input()
	in.Config = &AccountConfigInput{ReceiveMode: domain.Webhook}
	result, err := base.CreateAccount(context.Background(), owner, "create", in)
	if err != nil {
		t.Fatal(err)
	}
	account := result.Account.ID
	m := &preflightMemory{now: base.deps.Now(), epoch: "11111111-1111-4111-8111-111111111111", accounts: accounts, records: map[string]PreflightRecord{}, receipts: map[string]PreflightRequest{}, requester: true, session: true, tenant: true}
	s, err := NewPreflightService(PreflightDependencies{Store: m, Access: access, Accounts: accounts, Cipher: base.deps.Cipher, ScopeID: base.deps.ScopeID, SourceEpoch: m.epoch, NewID: base.deps.NewID})
	if err != nil {
		t.Fatal(err)
	}
	a := accounts.data.accounts[account]
	token := preflightToken(PreflightAccount{Credentials: a.Credentials})
	return s, m, account, channelv1.PreflightCreateRequest{ExpectedAccountRevision: a.Account.Revision, ExpectedConnectionRevision: a.Account.ConnectionRevision, ExpectedBotTokenVersion: token.Meta.Version}
}
func preflightInput(t *testing.T, seq int) channelv1.PreflightClaimRequest {
	t.Helper()
	origin := "https://gateway.example.com"
	r := channelv1.PreflightClaimRequest{DiagnosticPolicy: channelv1.PreflightReceiveModesPolicy, SchemaVersion: 1, ScopeID: "gateway_pool", SourceEpoch: "11111111-1111-4111-8111-111111111111", InstanceEpoch: "22222222-2222-4222-8222-222222222222", ClaimRequestID: fmt.Sprintf("33333333-3333-4333-8333-%012d", seq), ClaimToken: strings.Repeat("A", 43), ExpectedPublicOrigin: &origin, OriginStatus: "PUBLIC_ORIGIN_STATIC_VALID", Limit: 1}
	var err error
	r.GatewayConfigDigest, err = channelv1.PreflightConfigDigest(r.ScopeID, r.SourceEpoch, r.ExpectedPublicOrigin, r.OriginStatus)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func preflightPrincipal() WorkloadPrincipal {
	return WorkloadPrincipal{PrincipalID: "spiffe://trpc-agent-service/gateway/gw-1", ScopeID: "gateway_pool", InstanceID: "gw-1", Audience: WorkloadAudience, Consumers: []string{"telegram_preflight"}}
}
func preflightResolveInput(c channelv1.PreflightClaimRequest, epoch int64) channelv1.PreflightResolveRequest {
	return channelv1.PreflightResolveRequest{SchemaVersion: 1, ScopeID: c.ScopeID, SourceEpoch: c.SourceEpoch, InstanceEpoch: c.InstanceEpoch, LeaseEpoch: epoch, ClaimToken: c.ClaimToken}
}
func preflightCompleteInput(t *testing.T, c channelv1.PreflightClaimRequest, epoch int64) channelv1.PreflightCompleteRequest {
	t.Helper()
	raw, err := os.ReadFile("../../../../../api/schemas/channel/v1/fixtures/preflight-complete-valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Document channelv1.PreflightCompleteRequest `json:"document"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	r := fixture.Document
	r.ScopeID = c.ScopeID
	r.SourceEpoch = c.SourceEpoch
	r.InstanceEpoch = c.InstanceEpoch
	r.ClaimToken = c.ClaimToken
	r.LeaseEpoch = epoch
	r.GatewayConfigDigest = c.GatewayConfigDigest
	r.ExpectedPublicOrigin = c.ExpectedPublicOrigin
	if c.DiagnosticPolicy != "" {
		r.DiagnosticPolicy, r.ReceiveMode, r.ConnectionRevision, r.OriginStatus = c.DiagnosticPolicy, domain.Webhook, 1, c.OriginStatus
		r.EffectiveConfigDigest, _ = channelv1.PreflightEffectiveConfigDigest(c.ScopeID, c.SourceEpoch, r.ReceiveMode, r.ConnectionRevision, c.ExpectedPublicOrigin, c.OriginStatus)
	}
	return r
}
func requirePFError(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || err.Error() != code {
		t.Fatalf("error=%v want=%s", err, code)
	}
}
func createPF(t *testing.T, s *PreflightService, account string, in channelv1.PreflightCreateRequest) channelv1.PreflightCreated {
	t.Helper()
	r, err := s.Create(context.Background(), owner, account, "create-preflight", in)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func claimPF(t *testing.T, s *PreflightService, in channelv1.PreflightClaimRequest) *channelv1.PreflightGrant {
	t.Helper()
	g, err := s.Claim(context.Background(), preflightPrincipal(), in)
	if err != nil || g == nil {
		t.Fatalf("grant=%v err=%v", g, err)
	}
	return g
}

func TestPreflightApplicationLifecycleAndNoOperationalWrites(t *testing.T) {
	ctx := context.Background()
	s, m, account, input := preflightSetup(t)
	before := cloneState(m.accounts.data)
	created := createPF(t, s, account, input)
	replay, err := s.Create(ctx, owner, account, "create-preflight", input)
	if err != nil || replay != created {
		t.Fatal("create replay changed", err)
	}
	different := input
	different.ExpectedAccountRevision++
	_, err = s.Create(ctx, owner, account, "create-preflight", different)
	requirePFError(t, err, "CHANNEL_IDEMPOTENCY_CONFLICT")
	_, err = s.Create(ctx, Actor{owner.TenantID, "usr_other_owner"}, account, "create-preflight", input)
	requirePFError(t, err, "CHANNEL_IDEMPOTENCY_CONFLICT")
	claim := preflightInput(t, 1)
	grant := claimPF(t, s, claim)
	m.now = m.now.Add(time.Second)
	retry, err := s.Claim(ctx, preflightPrincipal(), claim)
	if err != nil || retry.LeaseEpoch != grant.LeaseEpoch || !retry.LeaseExpiresAt.Equal(grant.LeaseExpiresAt) || !retry.ServerTime.After(grant.ServerTime) {
		t.Fatal("claim replay extended lease or failed", err)
	}
	resolved, err := s.Resolve(ctx, preflightPrincipal(), created.PreflightID, preflightResolveInput(claim, 1))
	if err != nil || resolved.Value != "TEST_ONLY_BOT_TOKEN" || resolved.Purpose != domain.TelegramBotToken {
		t.Fatal("private resolve failed", err)
	}
	complete := preflightCompleteInput(t, claim, 1)
	complete.ObservedAt = m.now.Add(24 * time.Hour)
	if err = s.Complete(ctx, preflightPrincipal(), created.PreflightID, complete); err != nil {
		t.Fatal(err)
	}
	first := clonePF(m.records[created.PreflightID])
	if !first.View.CheckedAt.Equal(m.now) {
		t.Fatal("client time controlled completion")
	}
	m.requester = false
	m.session = false
	m.tenant = false
	m.now = m.now.Add(time.Hour)
	if err = s.Complete(ctx, preflightPrincipal(), created.PreflightID, complete); err != nil {
		t.Fatal("historical acknowledgement incorrectly reauthorized", err)
	}
	if !reflect.DeepEqual(first, m.records[created.PreflightID]) {
		t.Fatal("completion replay mutated fact")
	}
	complete.ObservedAt = complete.ObservedAt.Add(time.Second)
	requirePFError(t, s.Complete(ctx, preflightPrincipal(), created.PreflightID, complete), "CHANNEL_PREFLIGHT_RESULT_CONFLICT")
	m.tenant = true
	view, err := s.Get(ctx, owner, account, created.PreflightID)
	if err != nil || view.Freshness != "EXPIRED" || view.State != "COMPLETED" {
		t.Fatal("historical expiry", err)
	}
	raw, _ := json.Marshal(struct {
		Records  map[string]PreflightRecord
		Receipts map[string]PreflightRequest
		View     channelv1.PreflightView
	}{m.records, m.receipts, view})
	for _, secret := range []string{"TEST_ONLY_BOT_TOKEN", "TEST_ONLY_WEBHOOK_SECRET", claim.ClaimToken} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("secret retained")
		}
	}
	if !reflect.DeepEqual(before, m.accounts.data) {
		t.Fatal("preflight changed operational state")
	}
}
func TestPreflightApplicationLeaseReclaimAndConfigFence(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			ctx := context.Background()
			s, m, account, input := preflightSetup(t)
			created := createPF(t, s, account, input)
			claim := preflightInput(t, 1)
			first := claimPF(t, s, claim)
			m.now = m.now.Add(31 * time.Second)
			_, err := s.Claim(ctx, preflightPrincipal(), claim)
			requirePFError(t, err, "CHANNEL_PREFLIGHT_LEASE_EXPIRED")
			second := preflightInput(t, 2)
			second.ClaimToken = strings.Repeat("B", 42) + "A"
			if changed {
				second.ExpectedPublicOrigin = nil
				second.OriginStatus = "PUBLIC_ORIGIN_INVALID"
				second.GatewayConfigDigest, _ = channelv1.PreflightConfigDigest(second.ScopeID, second.SourceEpoch, nil, second.OriginStatus)
				_, err = s.Claim(ctx, preflightPrincipal(), second)
				requirePFError(t, err, "CHANNEL_PREFLIGHT_STALE")
				if m.records[created.PreflightID].View.State != "STALE" {
					t.Fatal("STALE rolled back")
				}
				return
			}
			grant := claimPF(t, s, second)
			if grant.LeaseEpoch != 2 || !m.records[created.PreflightID].View.StartedAt.Equal(first.ServerTime) {
				t.Fatal("reclaim reset first time")
			}
			_, err = s.Resolve(ctx, preflightPrincipal(), created.PreflightID, preflightResolveInput(claim, 1))
			requirePFError(t, err, "CHANNEL_PREFLIGHT_CLAIM_CONFLICT")
			m.now = m.now.Add(30 * time.Second)
			if err = s.Maintain(ctx); err != nil {
				t.Fatal(err)
			}
			r := m.records[created.PreflightID]
			if r.View.State != "TIMED_OUT" || r.View.ReasonCode != "CHANNEL_PREFLIGHT_EXECUTION_TIMEOUT" {
				t.Fatal("second lease did not terminate")
			}
		})
	}
}
func TestPreflightApplicationInvalidationAndMetadata(t *testing.T) {
	mutations := map[string]func(*preflightMemory, string){
		"requester": func(m *preflightMemory, _ string) { m.requester = false },
		"tenant":    func(m *preflightMemory, _ string) { m.tenant = false },
		"epoch":     func(m *preflightMemory, _ string) { m.epoch = "44444444-4444-4444-8444-444444444444" },
		"connection": func(m *preflightMemory, id string) {
			a := m.accounts.data.accounts[id]
			a.Account.Revision++
			a.Account.ConnectionRevision++
			m.accounts.data.accounts[id] = a
		},
		"enabled": func(m *preflightMemory, id string) {
			a := m.accounts.data.accounts[id]
			a.Account.Enabled = true
			m.accounts.data.accounts[id] = a
		},
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, m, account, in := preflightSetup(t)
			created := createPF(t, s, account, in)
			claim := preflightInput(t, 1)
			claimPF(t, s, claim)
			change(m, account)
			if err := s.Maintain(ctx); err != nil {
				t.Fatal(err)
			}
			if m.records[created.PreflightID].View.State != "STALE" {
				t.Fatal("not stale")
			}
			m.requester = true
			m.tenant = true
			m.epoch = s.deps.SourceEpoch
			if err := s.Maintain(ctx); err != nil {
				t.Fatal(err)
			}
			if m.records[created.PreflightID].View.State != "STALE" {
				t.Fatal("revived")
			}
		})
	}
	t.Run("metadata-only", func(t *testing.T) {
		ctx := context.Background()
		s, m, id, in := preflightSetup(t)
		created := createPF(t, s, id, in)
		claim := preflightInput(t, 1)
		claimPF(t, s, claim)
		a := m.accounts.data.accounts[id]
		a.Account.Revision++
		a.Account.Name = "renamed"
		m.accounts.data.accounts[id] = a
		v, err := s.Get(ctx, owner, id, created.PreflightID)
		if err != nil || v.State != "RUNNING" || !v.MetadataChanged {
			t.Fatal("metadata invalidation", err)
		}
		if err = s.Complete(ctx, preflightPrincipal(), created.PreflightID, preflightCompleteInput(t, claim, 1)); err != nil {
			t.Fatal(err)
		}
		a.Account.Revision++
		m.accounts.data.accounts[id] = a
		if _, err = s.Get(ctx, owner, id, created.PreflightID); err != nil {
			t.Fatal("terminal metadata mutation", err)
		}
	})
}
func TestPreflightApplicationTimeoutQuotasAndRollback(t *testing.T) {
	t.Run("no-executor-get", func(t *testing.T) {
		s, m, id, in := preflightSetup(t)
		created := createPF(t, s, id, in)
		m.now = m.now.Add(120 * time.Second)
		v, err := s.Get(context.Background(), owner, id, created.PreflightID)
		if err != nil || v.State != "TIMED_OUT" || v.ReasonCode != "CHANNEL_PREFLIGHT_NO_EXECUTOR" || v.GatewayConfigFreshness != nil {
			t.Fatal("no worker convergence", err)
		}
	})
	for _, counts := range [][3]int{{3, 3, 0}, {0, 30, 0}, {0, 0, 20}} {
		t.Run(fmt.Sprint(counts), func(t *testing.T) {
			s, m, id, in := preflightSetup(t)
			m.counts = &counts
			_, err := s.Create(context.Background(), owner, id, "rate", in)
			requirePFError(t, err, "CHANNEL_PREFLIGHT_RATE_LIMITED")
			if len(m.records) != 0 {
				t.Fatal("rate wrote task")
			}
		})
	}
	t.Run("receipt-rollback", func(t *testing.T) {
		s, m, id, in := preflightSetup(t)
		m.failReceipt = true
		_, err := s.Create(context.Background(), owner, id, "rollback", in)
		requirePFError(t, err, "CHANNEL_DEPENDENCY_UNAVAILABLE")
		if len(m.records) != 0 {
			t.Fatal("partial task committed")
		}
	})
	t.Run("claim-rate-and-empty-replay", func(t *testing.T) {
		ctx := context.Background()
		s, m, _, _ := preflightSetup(t)
		one, two, three := preflightInput(t, 1), preflightInput(t, 2), preflightInput(t, 3)
		for _, in := range []channelv1.PreflightClaimRequest{one, two, one} {
			g, err := s.Claim(ctx, preflightPrincipal(), in)
			if err != nil || g != nil {
				t.Fatal("empty replay", err)
			}
		}
		_, err := s.Claim(ctx, preflightPrincipal(), three)
		requirePFError(t, err, "CHANNEL_PREFLIGHT_RATE_LIMITED")
		three.InstanceEpoch = "55555555-5555-4555-8555-555555555555"
		_, err = s.Claim(ctx, preflightPrincipal(), three)
		requirePFError(t, err, "CHANNEL_PREFLIGHT_RATE_LIMITED")
		m.now = m.now.Add(time.Second)
		if _, err = s.Claim(ctx, preflightPrincipal(), three); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPreflightApplicationCompleteConfigChangeCommitsStale(t *testing.T) {
	ctx := context.Background()
	s, m, id, in := preflightSetup(t)
	created := createPF(t, s, id, in)
	claim := preflightInput(t, 1)
	claimPF(t, s, claim)
	original := preflightCompleteInput(t, claim, 1)
	changed := clonePF(original)
	origin := "https://other-gateway.example.com"
	changed.ExpectedPublicOrigin = &origin
	changed.GatewayConfigDigest, _ = channelv1.PreflightConfigDigest(claim.ScopeID, claim.SourceEpoch, &origin, claim.OriginStatus)
	changed.EffectiveConfigDigest, _ = channelv1.PreflightEffectiveConfigDigest(claim.ScopeID, claim.SourceEpoch, changed.ReceiveMode, changed.ConnectionRevision, &origin, claim.OriginStatus)
	requirePFError(t, s.Complete(ctx, preflightPrincipal(), created.PreflightID, changed), "CHANNEL_PREFLIGHT_STALE")
	view, err := s.Get(ctx, owner, id, created.PreflightID)
	if err != nil || view.State != "STALE" || view.ReasonCode != "CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED" {
		t.Fatal("configuration stale state rolled back", err)
	}
	if m.records[created.PreflightID].View.GatewayConfigDigest == nil || *m.records[created.PreflightID].View.GatewayConfigDigest != claim.GatewayConfigDigest {
		t.Fatal("pinned config changed")
	}
	requirePFError(t, s.Complete(ctx, preflightPrincipal(), created.PreflightID, original), "CHANNEL_PREFLIGHT_STALE")
}

func (m *preflightMemory) ActiveAccount(_ context.Context, scope, tenant, account string) (PreflightKey, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, r := range m.records {
		if r.ScopeID == scope && r.View.TenantID == tenant && r.View.AccountID == account && preflightActive(r) {
			return PreflightKey{id, tenant, account, r.View.RequestedBy}, true, nil
		}
	}
	return PreflightKey{}, false, nil
}

func TestPreflightApplicationRevokedCreateDoesNotMaintain(t *testing.T) {
	s, m, id, in := preflightSetup(t)
	created := createPF(t, s, id, in)
	before := clonePF(m.records[created.PreflightID])
	m.session = false
	m.now = m.now.Add(121 * time.Second)
	_, err := s.Create(context.Background(), owner, id, "revoked", in)
	requirePFError(t, err, "CHANNEL_PERMISSION_DENIED")
	if !reflect.DeepEqual(before, m.records[created.PreflightID]) {
		t.Fatal("unauthorized create mutated old task")
	}
	m.session = true
	replay, err := s.Create(context.Background(), owner, id, "create-preflight", in)
	if err != nil || replay != created {
		t.Fatal("authorized receipt replay failed", err)
	}
	if !reflect.DeepEqual(before, m.records[created.PreflightID]) {
		t.Fatal("receipt replay maintained current task")
	}
}

func TestPreflightApplicationHidesNonmemberAccountsAndTasks(t *testing.T) {
	ctx := context.Background()
	s, m, id, in := preflightSetup(t)
	created := createPF(t, s, id, in)
	beforeTasks, beforeReceipts := clonePF(m.records), clonePF(m.receipts)
	s.deps.Access = &access{allow: false, owner: false}
	_, err := s.Create(ctx, owner, id, "invisible", in)
	requirePFError(t, err, "CHANNEL_ACCOUNT_NOT_FOUND")
	_, err = s.Get(ctx, owner, id, created.PreflightID)
	requirePFError(t, err, "CHANNEL_ACCOUNT_NOT_FOUND")
	if !reflect.DeepEqual(beforeTasks, m.records) || !reflect.DeepEqual(beforeReceipts, m.receipts) {
		t.Fatal("invisible access wrote preflight state")
	}
	s.deps.Access = &access{allow: true, owner: false}
	if _, err = s.Get(ctx, owner, id, created.PreflightID); err != nil {
		t.Fatal("visible member cannot read", err)
	}
	_, err = s.Create(ctx, owner, id, "member", in)
	requirePFError(t, err, "CHANNEL_PERMISSION_DENIED")
}
