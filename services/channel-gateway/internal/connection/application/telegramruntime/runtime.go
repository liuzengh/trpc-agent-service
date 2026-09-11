// Package telegramruntime coordinates source-qualified, physically owned
// Telegram reception. Both modes feed the same durable admission boundary.
package telegramruntime

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	telegram "github.com/liuzengh/trpc-agent-service/platform/im/telegram"
	admission "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	use "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/accountuse"
	refresh "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/catalogrefresh"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/telegramreception"
)

var ErrPollingConflict = errors.New("telegram receiver: competing consumer")

type RateLimited struct{ After time.Duration }

func (e *RateLimited) Error() string { return "telegram receiver: rate limited" }

type Directory interface {
	Accounts() []c.Account
	Lookup(context.Context, string) (refresh.View, error)
}
type Installer interface {
	Install(c.Account, context.Context, string, int64) error
	Remove(string)
}
type Remote interface {
	Identity(context.Context) (string, error)
	Webhook(context.Context) (string, error)
	Register(context.Context, string, string) (bool, error)
	DeleteWebhook(context.Context) error
	Poll(context.Context, int64, int) ([]json.RawMessage, error)
	Close()
}
type RemoteFactory interface{ New(string) (Remote, error) }
type Receivers interface {
	Acquire(context.Context, *c.Permit, string) (d.Lease, bool, error)
	Check(context.Context, *c.Permit, d.Lease) error
	BeginCall(context.Context, *c.Permit, d.Lease) (d.Call, error)
	FinishCall(context.Context, *c.Permit, d.Lease, d.Call, bool) error
	CommitCursor(context.Context, *c.Permit, d.Lease, int64, int64) error
	RegistrationIntent(context.Context, *c.Permit, d.Lease, string) error
	ConfirmRegistration(context.Context, *c.Permit, d.Lease, string) error
	AdoptLegacy(context.Context, *c.Permit, d.Lease, string) (bool, error)
	Defer(context.Context, *c.Permit, d.Lease, time.Duration, string) error
	Ready(context.Context, *c.Permit) (bool, error)
	PollSucceeded(context.Context, *c.Permit, d.Lease) error
	PollingReady(context.Context, *c.Permit) (bool, error)
}
type Intake interface {
	Accept(context.Context, *c.Permit, d.Lease, json.RawMessage) error
}
type Status struct {
	AccountID     string
	Revision      int64
	State, Reason string
	OwnerEpoch    int64
	Healthy       bool
	Until         time.Time
}
type installed struct {
	revision, generation int64
	ctx                  context.Context
}
type Runtime struct {
	directory  Directory
	use        *use.Service
	installer  Installer
	receivers  Receivers
	factory    RemoteFactory
	intake     Intake
	origin     string
	mu         sync.Mutex
	entries    map[string]installed
	statuses   map[string]Status
	generation atomic.Int64
}

func New(dir Directory, u *use.Service, i Installer, r Receivers, f RemoteFactory, in Intake, origin string) (*Runtime, error) {
	if dir == nil || u == nil || i == nil || r == nil || f == nil || in == nil {
		return nil, c.ErrInvalid
	}
	return &Runtime{directory: dir, use: u, installer: i, receivers: r, factory: f, intake: in, origin: origin, entries: map[string]installed{}, statuses: map[string]Status{}}, nil
}
func (s *Runtime) set(a c.Account, state, reason string, epoch int64, healthy bool, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[a.ID] = Status{a.ID, a.ConnectionRevision, state, reason, epoch, healthy, until}
}
func (s *Runtime) Status(id string) Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statusLocked(id)
}

// statusLocked applies the lease expiry rule; callers already hold s.mu.
func (s *Runtime) statusLocked(id string) Status {
	st := s.statuses[id]
	if st.Healthy && !st.Until.IsZero() && !time.Now().Before(st.Until) {
		st.Healthy = false
		if st.State == "READY" {
			st.State = "CONNECTING"
			st.Reason = "OWNERSHIP_LOST"
			st.OwnerEpoch = 0
		}
	}
	return st
}

// AccountStatus pairs a catalog entry with its runtime status. Ready() fails
// when StatusRevision != ConnectionRevision or Healthy is false, so both sides
// of that comparison are reported together for operator diagnosis.
type AccountStatus struct {
	AccountID          string `json:"account_id"`
	Provider           string `json:"provider"`
	Enabled            bool   `json:"enabled"`
	ConnectionRevision int64  `json:"connection_revision"`
	StatusRevision     int64  `json:"status_revision"`
	State              string `json:"state"`
	Reason             string `json:"reason"`
	Healthy            bool   `json:"healthy"`
	OwnerEpoch         int64  `json:"owner_epoch"`
	Until              string `json:"until,omitempty"`
}

func (s *Runtime) AccountStatuses() []AccountStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AccountStatus, 0, len(s.statuses))
	for _, a := range s.directory.Accounts() {
		st := s.statusLocked(a.ID)
		until := ""
		if !st.Until.IsZero() {
			until = st.Until.UTC().Format(time.RFC3339)
		}
		out = append(out, AccountStatus{
			AccountID: a.ID, Provider: a.Provider, Enabled: a.Enabled,
			ConnectionRevision: a.ConnectionRevision, StatusRevision: st.Revision,
			State: st.State, Reason: st.Reason, Healthy: st.Healthy,
			OwnerEpoch: st.OwnerEpoch, Until: until,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccountID < out[j].AccountID })
	return out
}
func (s *Runtime) Ready() bool {
	for _, a := range s.directory.Accounts() {
		if a.Provider == "telegram" && a.Enabled {
			st := s.Status(a.ID)
			if st.Revision != a.ConnectionRevision || !st.Healthy {
				return false
			}
		}
	}
	return true
}

// admissionCause names why an admitted update was refused, using a fixed
// vocabulary rather than the error text so no inbound payload is echoed.
func admissionCause(e error) string {
	switch {
	case errors.Is(e, c.ErrUnauthorized):
		return "unauthorized"
	case errors.Is(e, c.ErrUnavailable):
		return "unavailable"
	case errors.Is(e, admission.ErrInvalidInput):
		return "invalid_input"
	case errors.Is(e, admission.ErrAccountUnavailable):
		return "account_unavailable"
	case errors.Is(e, d.ErrOwnership):
		return "ownership_lost"
	case errors.Is(e, admission.ErrRouteChanged):
		return "route_changed"
	case errors.Is(e, admission.ErrClaimLost):
		return "claim_lost"
	case errors.Is(e, admission.ErrUsageDenied):
		return "usage_denied"
	case errors.Is(e, admission.ErrRateLimited):
		return "rate_limited"
	case errors.Is(e, admission.ErrConflict):
		return "conflict"
	case errors.Is(e, admission.ErrUnavailable):
		return "admission_unavailable"
	case errors.Is(e, context.DeadlineExceeded):
		return "timeout"
	}
	return "other"
}

// providerCause names a failure class without echoing the error: transport
// errors embed the request URL, which carries the bot token.
func providerCause(e error) string {
	var apiErr *telegram.APIError
	if errors.As(e, &apiErr) {
		return "api" + strconv.Itoa(apiErr.Code)
	}
	var urlErr *url.Error
	if errors.As(e, &urlErr) {
		return "transport"
	}
	switch {
	case errors.Is(e, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(e, context.Canceled):
		return "canceled"
	}
	return "other"
}

func (s *Runtime) remove(id string) {
	s.installer.Remove(id)
	s.mu.Lock()
	delete(s.entries, id)
	s.mu.Unlock()
}
func (s *Runtime) install(ctx context.Context, a c.Account, lifetime context.Context, generation int64) (string, error) {
	p, account, e := s.use.Open(ctx, a.ID, "telegram_webhook", a.ConnectionRevision, generation)
	if e != nil {
		return "", e
	}
	defer p.Revoke()
	values, e := s.use.Resolve(ctx, p, account, nil, nil)
	if e != nil || len(values) != 1 {
		return "", c.ErrUnauthorized
	}
	secret := values[0].Value
	if lifetime.Err() != nil {
		return "", c.ErrUnauthorized
	}
	if e = s.installer.Install(a, lifetime, secret, generation); e != nil {
		return "", e
	}
	s.mu.Lock()
	s.entries[a.ID] = installed{a.ConnectionRevision, generation, lifetime}
	s.mu.Unlock()
	return secret, nil
}

// call fences every external method. Uncertain requests retain their recorded
// deadline; a different owner cannot infer quiescence from local cancellation.
func (s *Runtime) call(ctx context.Context, p *c.Permit, l d.Lease, fn func(context.Context) error) error {
	deadline, ok := p.Context().Deadline()
	if !ok || time.Until(deadline) < d.CallBudget+d.QuiescenceMargin {
		return d.ErrOwnership
	}
	call, e := s.receivers.BeginCall(ctx, p, l)
	if e != nil {
		return e
	}
	op, cancel := context.WithDeadline(ctx, call.Deadline)
	defer cancel()
	stop := context.AfterFunc(p.Context(), cancel)
	defer stop()
	if op.Err() != nil || p.Check() != nil || !time.Now().Before(call.Deadline) {
		return d.ErrOwnership
	}
	e = fn(op)
	settle, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer done()
	finish := s.receivers.FinishCall(settle, p, l, call, e == nil)
	if e != nil {
		return e
	}
	return finish
}
func (s *Runtime) reconcile(ctx context.Context, listed c.Account) {
	v, e := s.directory.Lookup(ctx, listed.ID)
	if e != nil || !listed.Enabled {
		s.remove(listed.ID)
		if !listed.Enabled {
			s.set(listed, "DISABLED", "NONE", 0, false, time.Time{})
		} else {
			s.set(listed, "ERROR", "SOURCE_UNAVAILABLE", 0, false, time.Time{})
		}
		return
	}
	a := v.Account
	if !a.Enabled {
		s.remove(a.ID)
		s.set(a, "DISABLED", "NONE", 0, false, time.Time{})
		return
	}
	mode := a.ReceiveMode()
	if a.ID != listed.ID || (mode != "webhook" && mode != "long_polling") {
		s.remove(listed.ID)
		s.set(listed, "ERROR", "CONFIG_INVALID", 0, false, time.Time{})
		return
	}
	s.mu.Lock()
	old, installed := s.entries[a.ID]
	s.mu.Unlock()
	if mode != "webhook" || installed && (old.revision != a.ConnectionRevision || old.ctx.Err() != nil || old.ctx != v.Context) {
		s.remove(a.ID)
		installed = false
	}
	if mode == "webhook" && !validOrigin(s.origin) {
		s.remove(a.ID)
		s.set(a, "ERROR", "CONFIG_INVALID", 0, false, time.Time{})
		return
	}
	generation := s.generation.Add(1)
	p, account, e := s.use.Open(ctx, a.ID, "telegram_receiver", a.ConnectionRevision, generation)
	if e != nil {
		s.set(a, "ERROR", "CREDENTIAL_UNAVAILABLE", 0, false, time.Time{})
		return
	}
	defer p.Revoke()
	l, acquired, e := s.receivers.Acquire(ctx, p, a.ProviderAccountID)
	if e != nil {
		s.set(a, "ERROR", "OWNERSHIP_LOST", 0, false, time.Time{})
		return
	}
	if !acquired {
		if mode == "webhook" {
			ready, e := s.receivers.Ready(ctx, p)
			if e == nil && ready {
				if !installed {
					if _, e = s.install(ctx, a, v.Context, generation); e != nil {
						s.set(a, "ERROR", "CREDENTIAL_UNAVAILABLE", 0, false, time.Time{})
						return
					}
				}
				s.set(a, "READY", "NONE", 0, true, time.Now().Add(5*time.Second))
				return
			}
		}
		healthy := false
		if mode == "long_polling" {
			healthy, _ = s.receivers.PollingReady(ctx, p)
		}
		// Not owning the lease right now is neutral pacing, not a diagnosis. A
		// recorded failure from the previous cycle must stay visible: Ready()
		// fails on it, so overwriting it here would hide the only reason an
		// operator can see for the Gateway reporting unready.
		if previous := s.Status(a.ID); previous.State == "ERROR" && previous.Reason != "" {
			return
		}
		s.set(a, "CONFIG_APPLIED", "WAITING_FOR_OWNER", 0, healthy, time.Now().Add(5*time.Second))
		return
	}
	reason := "PROVIDER_UNAVAILABLE"
	delay := 5 * time.Second
	success := false
	defer func() {
		settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if e := s.receivers.Defer(settle, p, l, delay, reason); e != nil {
			s.set(a, "ERROR", "OWNERSHIP_LOST", 0, false, time.Time{})
			return
		}
		if success {
			s.set(a, "READY", "NONE", l.Epoch, true, l.Until)
		} else {
			s.set(a, "ERROR", reason, 0, false, time.Time{})
		}
	}()
	if e = s.receivers.Check(ctx, p, l); e != nil {
		reason = "OWNERSHIP_LOST"
		return
	}
	owner := connection.OwnerGrant{AccountID: a.ID, InstanceID: l.InstanceID, Revision: l.Revision, Epoch: l.Epoch}
	values, e := s.use.Resolve(ctx, p, account, &owner, nil)
	if e != nil || len(values) != 1 {
		reason = "CREDENTIAL_UNAVAILABLE"
		return
	}
	var remote Remote
	if f, ok := s.factory.(interface {
		NewAccount(string, c.Account) (Remote, error)
	}); ok {
		remote, e = f.NewAccount(values[0].Value, a)
	} else if a.Config.EndpointProfile == "test" {
		e = c.ErrInvalid
	} else {
		remote, e = s.factory.New(values[0].Value)
	}
	values[0].Value = ""
	if e != nil {
		reason = "CREDENTIAL_UNAVAILABLE"
		return
	}
	defer remote.Close()
	classify := func(e error) {
		var rate *RateLimited
		if errors.Is(e, ErrPollingConflict) {
			reason = "POLLING_CONFLICT"
			delay = 30 * time.Second
		} else if errors.As(e, &rate) {
			reason = "PROVIDER_RATE_LIMITED"
			delay = max(5*time.Second, min(rate.After, 24*time.Hour))
		} else if errors.Is(e, d.ErrOwnership) {
			reason = "OWNERSHIP_LOST"
		} else {
			// Every unclassified failure would otherwise collapse into the same
			// PROVIDER_UNAVAILABLE code, hiding why the Gateway is unready.
			reason = "PROVIDER_UNAVAILABLE:" + providerCause(e)
		}
	}
	var id string
	if e = s.call(ctx, p, l, func(call context.Context) error { var e error; id, e = remote.Identity(call); return e }); e != nil {
		classify(e)
		return
	}
	if id != a.ProviderAccountID {
		reason = "REMOTE_IDENTITY_MISMATCH"
		return
	}
	var actual string
	if e = s.call(ctx, p, l, func(call context.Context) error { var e error; actual, e = remote.Webhook(call); return e }); e != nil {
		classify(e)
		return
	}
	expected := ""
	if validOrigin(s.origin) {
		expected = s.origin + a.Config.WebhookPath
	}
	if actual != "" && l.ManagedURL == "" && l.PendingURL == "" && expected != "" && actual == expected {
		adopted, e := s.receivers.AdoptLegacy(ctx, p, l, expected)
		if e != nil {
			return
		}
		if adopted {
			l.ManagedURL = expected
		}
	}
	if !d.ManagedRemote(actual, l.ManagedURL) && !d.ManagedRemote(actual, l.PendingURL) {
		reason = "WEBHOOK_CONFLICT"
		delay = 30 * time.Second
		return
	}
	if mode == "webhook" {
		secret, e := s.install(ctx, a, v.Context, generation)
		if e != nil {
			reason = "CREDENTIAL_UNAVAILABLE"
			return
		}
		if e = s.receivers.RegistrationIntent(ctx, p, l, expected); e != nil {
			reason = "OWNERSHIP_LOST"
			return
		}
		if e = s.call(ctx, p, l, func(call context.Context) error {
			ok, e := remote.Register(call, expected, secret)
			if e == nil && !ok {
				return c.ErrUnavailable
			}
			return e
		}); e != nil {
			reason = "REGISTRATION_UNKNOWN"
			classify(e)
			return
		}
		secret = ""
		if e = s.call(ctx, p, l, func(call context.Context) error { var e error; actual, e = remote.Webhook(call); return e }); e != nil {
			reason = "REGISTRATION_UNKNOWN"
			classify(e)
			return
		}
		if actual != expected {
			reason = "WEBHOOK_CONFLICT"
			return
		}
		if e = s.receivers.ConfirmRegistration(ctx, p, l, expected); e != nil {
			reason = "OWNERSHIP_LOST"
			return
		}
		success = true
		reason = "NONE"
		delay = 30 * time.Second
		return
	}
	if actual != "" {
		if e = s.call(ctx, p, l, remote.DeleteWebhook); e != nil {
			reason = "REGISTRATION_UNKNOWN"
			classify(e)
			return
		}
		if e = s.call(ctx, p, l, func(call context.Context) error { var e error; actual, e = remote.Webhook(call); return e }); e != nil {
			classify(e)
			return
		}
		if actual != "" {
			reason = "WEBHOOK_CONFLICT"
			return
		}
	}
	var updates []json.RawMessage
	if e = s.call(ctx, p, l, func(call context.Context) error {
		var e error
		updates, e = remote.Poll(call, l.PollOffset(time.Now()), 3)
		return e
	}); e != nil {
		classify(e)
		return
	}
	expectedOffset := l.NextOffset
	for _, raw := range updates {
		var update struct {
			ID *int64 `json:"update_id"`
		}
		if json.Unmarshal(raw, &update) != nil {
			reason = "UPDATE_MALFORMED"
			return
		}
		if update.ID == nil || *update.ID < 0 || *update.ID >= c.MaxRevision {
			// A bare return here would leave reason at its initializer and report
			// PROVIDER_UNAVAILABLE, naming the wrong subsystem: the provider call
			// succeeded and its answer is what this loop rejected.
			reason = "UPDATE_ID_INVALID"
			return
		}
		if e = s.receivers.Check(ctx, p, l); e != nil {
			reason = "OWNERSHIP_LOST"
			return
		}
		if e = s.intake.Accept(ctx, p, l, raw); e != nil {
			reason = "ADMISSION_REJECTED:" + admissionCause(e)
			return
		}
		next := *update.ID + 1
		if e = s.receivers.CommitCursor(ctx, p, l, expectedOffset, next); e != nil {
			reason = "OWNERSHIP_LOST"
			return
		}
		expectedOffset = next
	}
	if e = s.receivers.PollSucceeded(ctx, p, l); e != nil {
		reason = "OWNERSHIP_LOST"
		return
	}
	success = true
	reason = "NONE"
	delay = 0
}
func validOrigin(raw string) bool {
	u, e := url.Parse(raw)
	return e == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.RawPath == "" && u.Path == ""
}

// Run uses bounded workers and joins a sweep before starting another. No hidden
// SDK goroutines may continue polling after the receiver exits.
func (s *Runtime) Run(ctx context.Context) error {
	defer func() {
		for _, a := range s.directory.Accounts() {
			if a.Provider == "telegram" {
				s.remove(a.ID)
				s.set(a, "ERROR", "SHUTDOWN", 0, false, time.Time{})
			}
		}
	}()
	for ctx.Err() == nil {
		accounts := s.directory.Accounts()
		present := map[string]bool{}
		for _, a := range accounts {
			present[a.ID] = true
		}
		s.mu.Lock()
		for id := range s.entries {
			if !present[id] {
				s.installer.Remove(id)
				delete(s.entries, id)
				delete(s.statuses, id)
			}
		}
		s.mu.Unlock()
		jobs := make(chan c.Account)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for a := range jobs {
					s.reconcile(ctx, a)
				}
			}()
		}
		for _, a := range accounts {
			if a.Provider == "telegram" {
				select {
				case jobs <- a:
				case <-ctx.Done():
				}
			}
		}
		close(jobs)
		wg.Wait()
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
