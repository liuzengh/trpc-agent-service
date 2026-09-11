package telegramruntime

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	use "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/accountuse"
	refresh "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/catalogrefresh"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/telegramreception"
)

const fixtureEpoch = "00000000-0000-4000-8000-000000000001"

type directoryFixture struct {
	account  c.Account
	lifetime context.Context
}

func (f *directoryFixture) Accounts() []c.Account { return []c.Account{f.account} }
func (f *directoryFixture) Lookup(context.Context, string) (refresh.View, error) {
	return refresh.View{Account: f.account, Qualification: c.Qualification{Generation: 1}, Context: f.lifetime}, nil
}

type issuerFixture struct{}

func (issuerFixture) Issue(parent context.Context, p c.Poll, q c.Qualification, a c.Account, kind string, generation int64) (*c.Permit, error) {
	return c.NewPermit(parent, c.UseBinding{ScopeID: "pool", SourceEpoch: fixtureEpoch, InstanceID: "gw", InstanceEpoch: fixtureEpoch, TenantID: a.TenantID, Provider: a.Provider, AccountID: a.ID, Kind: kind, ConnectionRevision: a.ConnectionRevision, QualificationGeneration: 1, ClientGeneration: generation}, time.Now().Add(30*time.Second))
}

type credentialsFixture struct{ calls []string }

func (f *credentialsFixture) ResolveValues(ctx context.Context, a c.Account, r c.ResolveRequest) ([]use.Value, error) {
	if e := r.Validate(a); e != nil {
		return nil, e
	}
	f.calls = append(f.calls, r.Consumer.Kind)
	out := []use.Value{}
	for _, u := range r.Uses {
		out = append(out, use.Value{Purpose: u.Purpose, ID: u.ID, Version: u.Version, Value: "synthetic"})
	}
	return out, nil
}

type installerFixture struct {
	installed int
	removed   int
}

func (f *installerFixture) Install(c.Account, context.Context, string, int64) error {
	f.installed++
	return nil
}
func (f *installerFixture) Remove(string) { f.removed++ }

type receiverFixture struct {
	lease      d.Lease
	acquired   bool
	lost       bool
	cursorFail bool
	commits    []int64
	uncertain  int
	polled     bool
	reason     string
	deferred   time.Duration
}

func (f *receiverFixture) Acquire(ctx context.Context, p *c.Permit, bot string) (d.Lease, bool, error) {
	l := f.lease
	l.Revision = p.Binding().ConnectionRevision
	return l, f.acquired, nil
}
func (f *receiverFixture) Check(context.Context, *c.Permit, d.Lease) error {
	if f.lost {
		return d.ErrOwnership
	}
	return nil
}
func (f *receiverFixture) BeginCall(ctx context.Context, p *c.Permit, l d.Lease) (d.Call, error) {
	if f.lost {
		return d.Call{}, d.ErrOwnership
	}
	return d.Call{ID: "call", Deadline: time.Now().Add(8 * time.Second)}, nil
}
func (f *receiverFixture) FinishCall(ctx context.Context, p *c.Permit, l d.Lease, call d.Call, complete bool) error {
	if !complete {
		f.uncertain++
	}
	return nil
}
func (f *receiverFixture) CommitCursor(ctx context.Context, p *c.Permit, l d.Lease, old, next int64) error {
	if f.cursorFail || f.lost {
		return d.ErrCursor
	}
	if old != f.lease.NextOffset {
		return d.ErrCursor
	}
	f.lease.NextOffset = next
	f.commits = append(f.commits, next)
	return nil
}
func (f *receiverFixture) RegistrationIntent(ctx context.Context, p *c.Permit, l d.Lease, address string) error {
	f.lease.PendingURL = address
	return nil
}
func (f *receiverFixture) ConfirmRegistration(ctx context.Context, p *c.Permit, l d.Lease, address string) error {
	f.lease.ManagedURL = address
	f.lease.PendingURL = ""
	return nil
}
func (f *receiverFixture) AdoptLegacy(context.Context, *c.Permit, d.Lease, string) (bool, error) {
	return false, nil
}
func (f *receiverFixture) Defer(ctx context.Context, p *c.Permit, l d.Lease, delay time.Duration, reason string) error {
	f.reason = reason
	f.deferred = delay
	return nil
}
func (f *receiverFixture) Ready(context.Context, *c.Permit) (bool, error) { return false, nil }
func (f *receiverFixture) PollingReady(context.Context, *c.Permit) (bool, error) {
	return f.polled, nil
}
func (f *receiverFixture) PollSucceeded(context.Context, *c.Permit, d.Lease) error {
	f.polled = true
	return nil
}

type remoteFixture struct {
	identity, url                 string
	updates                       []json.RawMessage
	pollError                     error
	polls, deletes, registrations int
	offsets                       []int64
}

func (f *remoteFixture) New(string) (Remote, error)               { return f, nil }
func (f *remoteFixture) Identity(context.Context) (string, error) { return f.identity, nil }
func (f *remoteFixture) Webhook(context.Context) (string, error)  { return f.url, nil }
func (f *remoteFixture) Register(ctx context.Context, address, secret string) (bool, error) {
	f.registrations++
	f.url = address
	return true, nil
}
func (f *remoteFixture) DeleteWebhook(context.Context) error { f.deletes++; f.url = ""; return nil }
func (f *remoteFixture) Poll(ctx context.Context, offset int64, timeout int) ([]json.RawMessage, error) {
	f.polls++
	f.offsets = append(f.offsets, offset)
	out := []json.RawMessage{}
	for _, raw := range f.updates {
		var u struct {
			ID int64 `json:"update_id"`
		}
		_ = json.Unmarshal(raw, &u)
		if u.ID >= offset {
			out = append(out, raw)
		}
	}
	return out, f.pollError
}
func (f *remoteFixture) Close() {}

type intakeFunc func(context.Context, *c.Permit, d.Lease, json.RawMessage) error

func (f intakeFunc) Accept(ctx context.Context, p *c.Permit, l d.Lease, raw json.RawMessage) error {
	return f(ctx, p, l, raw)
}
func fixture(t *testing.T, in intakeFunc) (*Runtime, *directoryFixture, *receiverFixture, *remoteFixture, *credentialsFixture) {
	t.Helper()
	a := c.Account{TenantID: "tenant", ID: "account", Provider: "telegram", ProviderAccountID: "123", Revision: 1, ConnectionRevision: 1, Enabled: true, Config: c.Config{ReceiveMode: "long_polling", WebhookPath: "/v1/telegram/account"}, Credentials: []c.Credential{{Purpose: "telegram.bot_token", ID: "token", Version: 1, Configured: true}, {Purpose: "telegram.webhook_secret", ID: "secret", Version: 1}}}
	dir := &directoryFixture{a, context.Background()}
	creds := &credentialsFixture{}
	r := &receiverFixture{acquired: true, lease: d.Lease{BotID: "123", ScopeID: "pool", AccountID: "account", InstanceID: "gw", InstanceEpoch: fixtureEpoch, SourceEpoch: fixtureEpoch, Revision: 1, Epoch: 1, Until: time.Now().Add(30 * time.Second), LastUpdateAt: time.Now()}}
	remote := &remoteFixture{identity: "123"}
	u := &use.Service{Directory: dir, Issuer: issuerFixture{}, Credentials: creds}
	s, e := New(dir, u, &installerFixture{}, r, remote, in, "")
	if e != nil {
		t.Fatal(e)
	}
	return s, dir, r, remote, creds
}
func rawID(raw json.RawMessage) int64 {
	var u struct {
		ID int64 `json:"update_id"`
	}
	_ = json.Unmarshal(raw, &u)
	return u.ID
}
func updates(ids ...int64) []json.RawMessage {
	var out []json.RawMessage
	for _, id := range ids {
		out = append(out, json.RawMessage(`{"update_id":`+strconv.FormatInt(id, 10)+`}`))
	}
	return out
}
func TestAdmissionFailureDoesNotConfirmLaterUpdates(t *testing.T) {
	accepted := []int64{}
	fail := true
	s, dir, store, remote, creds := fixture(t, func(ctx context.Context, p *c.Permit, l d.Lease, raw json.RawMessage) error {
		id := rawID(raw)
		if id == 102 && fail {
			return errors.New("database unavailable")
		}
		accepted = append(accepted, id)
		return nil
	})
	remote.updates = updates(101, 102, 103)
	s.reconcile(context.Background(), dir.account)
	if store.lease.NextOffset != 102 || len(accepted) != 1 || accepted[0] != 101 {
		t.Fatalf("cursor=%d accepted=%v", store.lease.NextOffset, accepted)
	}
	fail = false
	s.reconcile(context.Background(), dir.account)
	if store.lease.NextOffset != 104 || remote.offsets[1] != 102 || len(accepted) != 3 {
		t.Fatalf("cursor=%d offsets=%v accepted=%v", store.lease.NextOffset, remote.offsets, accepted)
	}
	if !s.Ready() || remote.deletes != 0 || remote.registrations != 0 {
		t.Fatal("pure polling required webhook side effects")
	}
	for _, kind := range creds.calls {
		if kind != "telegram_receiver" {
			t.Fatal("LP requested a webhook credential")
		}
	}
}
func TestReceiptThenCursorFailureReplaysWithoutSkipping(t *testing.T) {
	seen := map[int64]int{}
	s, dir, store, remote, _ := fixture(t, func(ctx context.Context, p *c.Permit, l d.Lease, raw json.RawMessage) error {
		seen[rawID(raw)]++
		return nil
	})
	remote.updates = updates(201, 205)
	store.cursorFail = true
	s.reconcile(context.Background(), dir.account)
	if store.lease.NextOffset != 0 || seen[205] != 0 {
		t.Fatal("failed CAS skipped update")
	}
	store.cursorFail = false
	s.reconcile(context.Background(), dir.account)
	if seen[201] != 2 || seen[205] != 1 || store.lease.NextOffset != 206 {
		t.Fatal(seen, store.lease.NextOffset)
	}
}
func TestOnlyManagedWebhookCanBeRemoved(t *testing.T) {
	for _, managed := range []bool{false, true} {
		t.Run(strconv.FormatBool(managed), func(t *testing.T) {
			s, dir, store, remote, _ := fixture(t, func(context.Context, *c.Permit, d.Lease, json.RawMessage) error { return nil })
			remote.url = "https://old.example/receiver"
			if managed {
				store.lease.ManagedURL = remote.url
			}
			s.reconcile(context.Background(), dir.account)
			if managed {
				if remote.deletes != 1 || remote.polls != 1 {
					t.Fatal("managed transition did not complete")
				}
			} else if remote.deletes != 0 || remote.polls != 0 || s.Status("account").Reason != "WEBHOOK_CONFLICT" {
				t.Fatal("unmanaged endpoint was taken over")
			}
		})
	}
}
func TestStandbyAndConflictingPoller(t *testing.T) {
	s, dir, store, remote, _ := fixture(t, func(context.Context, *c.Permit, d.Lease, json.RawMessage) error { return nil })
	store.acquired = false
	s.reconcile(context.Background(), dir.account)
	if remote.polls != 0 || s.Status("account").OwnerEpoch != 0 || s.Status("account").Reason != "WAITING_FOR_OWNER" {
		t.Fatal("standby claimed ownership")
	}
	store.acquired = true
	remote.pollError = ErrPollingConflict
	s.reconcile(context.Background(), dir.account)
	if store.uncertain != 1 || store.lease.NextOffset != 0 || store.deferred != 30*time.Second || store.reason != "POLLING_CONFLICT" {
		t.Fatal("conflict not fenced/backed off")
	}
}
func TestIdleRecoveryAndIdentityMismatch(t *testing.T) {
	s, dir, store, remote, _ := fixture(t, func(context.Context, *c.Permit, d.Lease, json.RawMessage) error { return nil })
	store.lease.NextOffset = 900000
	store.lease.LastUpdateAt = time.Now().Add(-7 * 24 * time.Hour)
	remote.updates = updates(42)
	s.reconcile(context.Background(), dir.account)
	if remote.offsets[0] != 0 || store.lease.NextOffset != 43 {
		t.Fatal("random idle update was prematurely acknowledged")
	}
	remote.identity = "999"
	s.reconcile(context.Background(), dir.account)
	if remote.polls != 1 || store.reason != "REMOTE_IDENTITY_MISMATCH" {
		t.Fatal("identity mismatch consumed updates")
	}
}
func TestDisabledAndExpiredOwnerNeverAdvance(t *testing.T) {
	var store *receiverFixture
	s, dir, r, remote, _ := fixture(t, func(context.Context, *c.Permit, d.Lease, json.RawMessage) error { store.lost = true; return nil })
	store = r
	remote.updates = updates(1)
	s.reconcile(context.Background(), dir.account)
	if store.lease.NextOffset != 0 {
		t.Fatal("lost owner advanced")
	}
	dir.account.Enabled = false
	s.reconcile(context.Background(), dir.account)
	if remote.polls != 1 || s.Status("account").State != "DISABLED" {
		t.Fatal("disabled polled")
	}
}
