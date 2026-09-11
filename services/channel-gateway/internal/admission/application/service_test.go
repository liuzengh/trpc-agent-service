package application

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

type usagePolicyFunc func(context.Context, string) (governancev1.Policy, error)

func (f usagePolicyFunc) UsagePolicy(ctx context.Context, tenant string) (governancev1.Policy, error) {
	return f(ctx, tenant)
}

type ledgerStub struct {
	find   func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error)
	commit func(context.Context, domain.Acceptance) (domain.Receipt, error)
}

func (l ledgerStub) Find(ctx context.Context, k domain.EventKey) (domain.Receipt, string, bool, error) {
	if l.find != nil {
		return l.find(ctx, k)
	}
	return domain.Receipt{}, "", false, nil
}
func (l ledgerStub) Commit(ctx context.Context, c domain.Acceptance) (domain.Receipt, error) {
	if l.commit != nil {
		return l.commit(ctx, c)
	}
	return c.Receipt, nil
}

type resolveFunc func(context.Context, string, string) (domain.RouteSnapshot, error)

func (f resolveFunc) Resolve(ctx context.Context, p, a string) (domain.RouteSnapshot, error) {
	return f(ctx, p, a)
}

type cohortResolver struct {
	resolveFor func(context.Context, string, string, string, string, string) (domain.RouteSnapshot, error)
}

func (r cohortResolver) Resolve(context.Context, string, string) (domain.RouteSnapshot, error) {
	return domain.RouteSnapshot{}, errors.New("legacy resolver must not be used")
}
func (r cohortResolver) ResolveFor(ctx context.Context, provider, account, conversation, thread, sender string) (domain.RouteSnapshot, error) {
	return r.resolveFor(ctx, provider, account, conversation, thread, sender)
}
func input() domain.Inbound {
	return domain.Inbound{Key: domain.EventKey{Provider: "telegram", AccountID: "account", EventID: "event"}, Kind: "text", ConversationID: "100", SenderID: "100", Text: "hello", SourceDigest: strings.Repeat("a", 64), ReceivedAt: time.Now().UTC()}
}
func route() domain.RouteSnapshot {
	return domain.RouteSnapshot{Provider: "telegram", AccountID: "account", TenantID: "tenant", BindingID: "binding", Generation: 1, DeploymentRevisionID: "revision", ManifestRef: "manifests/revision", ManifestDigest: "sha256:" + strings.Repeat("b", 64)}
}
func routes() resolveFunc {
	return func(context.Context, string, string) (domain.RouteSnapshot, error) { return route(), nil }
}
func noRoutes(t *testing.T) resolveFunc {
	t.Helper()
	return func(context.Context, string, string) (domain.RouteSnapshot, error) {
		t.Error("unexpected route resolution")
		return domain.RouteSnapshot{}, errors.New("unexpected")
	}
}

func usagePolicy(allowed string) governancev1.Policy {
	return governancev1.Policy{
		SchemaVersion: 1, TenantID: "tenant", Revision: 3, Enabled: true,
		IM:        governancev1.IMPolicy{Rules: []governancev1.IMRule{{AccountID: "account", BindingID: "binding", UserIDs: []string{allowed}, GroupIDs: []string{}}}},
		Requests:  governancev1.RequestPolicy{TenantPerMinute: 10, UserPerMinute: 2},
		Execution: governancev1.ExecutionPolicy{MaxConcurrentRuns: 2},
		Tokens:    governancev1.TokenPolicy{PeriodSeconds: 3600, Limit: 10000, ReservationPerRun: 100},
	}
}

func TestUsagePolicyAuthorizesBeforeAdmissionAndFreezesSnapshot(t *testing.T) {
	in := input()
	commits := 0
	service := New(ledgerStub{commit: func(_ context.Context, c domain.Acceptance) (domain.Receipt, error) {
		commits++
		if c.Policy == nil || c.Policy.Revision != 3 || c.Policy.Requests.UserPerMinute != 2 {
			t.Fatalf("policy snapshot not fixed: %+v", c.Policy)
		}
		return c.Receipt, nil
	}}, routes()).WithUsagePolicies(usagePolicyFunc(func(context.Context, string) (governancev1.Policy, error) {
		return usagePolicy(in.SenderID), nil
	}))
	if _, err := service.AcceptInbound(context.Background(), in); err != nil || commits != 1 {
		t.Fatalf("authorized admission err=%v commits=%d", err, commits)
	}

	denied := New(ledgerStub{}, routes()).WithUsagePolicies(usagePolicyFunc(func(context.Context, string) (governancev1.Policy, error) {
		return usagePolicy("someone-else"), nil
	}))
	if _, err := denied.AcceptInbound(context.Background(), in); !errors.Is(err, domain.ErrUsageDenied) {
		t.Fatalf("denied identity: %v", err)
	}
}

func TestReplayPrecedesRoutingAndStopping(t *testing.T) {
	in := input()
	old := domain.Receipt{Decision: "admit-run", AdmissionID: "old-admission", RunID: "old-run"}
	service := New(ledgerStub{find: func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error) {
		return old, strings.Repeat("a", 64), true, nil
	}, commit: func(context.Context, domain.Acceptance) (domain.Receipt, error) {
		t.Fatal("replayed input committed again")
		return domain.Receipt{}, nil
	}}, noRoutes(t))
	service.Stop()
	got, err := service.AcceptInbound(context.Background(), in)
	if err != nil || got != old {
		t.Fatalf("replay: %+v %v", got, err)
	}
	in.SourceDigest = strings.Repeat("b", 64)
	if _, err = service.AcceptInbound(context.Background(), in); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("digest conflict: %v", err)
	}
}
func TestDecisionsOwnTheirFacts(t *testing.T) {
	for _, kind := range []string{"text", "ignore", "interaction"} {
		t.Run(kind, func(t *testing.T) {
			in := input()
			in.Kind = kind
			if kind != "text" {
				in.Text = ""
			}
			calls := 0
			resolver := noRoutes(t)
			if kind == "text" {
				resolver = routes()
			}
			service := New(ledgerStub{commit: func(_ context.Context, c domain.Acceptance) (domain.Receipt, error) {
				calls++
				if err := c.Validate(); err != nil {
					t.Fatalf("acceptance invalid: %v", err)
				}
				if c.Input.Key != in.Key || c.Input.SourceDigest != in.SourceDigest {
					t.Fatal("input identity changed")
				}
				if kind == "text" {
					if c.Route == nil || c.Route.Generation != 1 || c.Receipt.RunID == "" || c.Receipt.AdmissionID == "" || c.Receipt.RunID == c.Receipt.AdmissionID {
						t.Fatalf("missing stable facts: %+v", c)
					}
				} else if c.Route != nil || c.Receipt.RunID != "" {
					t.Fatal("non-run acquired routing facts")
				}
				return c.Receipt, nil
			}}, resolver)
			got, err := service.AcceptInbound(context.Background(), in)
			if err != nil || calls != 1 {
				t.Fatalf("acceptance: %+v %v calls=%d", got, err, calls)
			}
			want := kind
			if want == "text" {
				want = "admit-run"
			}
			if got.Decision != want {
				t.Fatalf("decision: %s", got.Decision)
			}
		})
	}
}

func TestAdmissionPersistsSelectedCanaryTarget(t *testing.T) {
	in := input()
	committed := false
	service := New(ledgerStub{commit: func(_ context.Context, c domain.Acceptance) (domain.Receipt, error) {
		committed = true
		if c.Route == nil || c.Route.DeploymentRevisionID != "revision-canary" || c.Route.ManifestRef != "manifests/revision-canary" || c.Route.RolloutID != "rollout-1" || c.Route.RolloutVariant != "canary" {
			t.Fatalf("selected target was not fixed in admission: %+v", c.Route)
		}
		return c.Receipt, nil
	}}, cohortResolver{resolveFor: func(_ context.Context, provider, account, conversation, thread, sender string) (domain.RouteSnapshot, error) {
		if provider != in.Key.Provider || account != in.Key.AccountID || conversation != in.ConversationID || thread != in.ThreadID || sender != in.SenderID {
			t.Fatalf("untrusted or incomplete cohort: %q %q %q %q %q", provider, account, conversation, thread, sender)
		}
		r := route()
		r.DeploymentRevisionID = "revision-canary"
		r.ManifestRef = "manifests/revision-canary"
		r.RolloutID = "rollout-1"
		r.RolloutVariant = "canary"
		return r, nil
	}})

	if _, err := service.AcceptInbound(context.Background(), in); err != nil || !committed {
		t.Fatalf("admission err=%v committed=%v", err, committed)
	}
}
func TestRouteRacesAreRetriedButBounded(t *testing.T) {
	for _, successAt := range []int{2, 4} {
		t.Run(string(rune('0'+successAt)), func(t *testing.T) {
			generation := int64(0)
			commits := 0
			service := New(ledgerStub{commit: func(_ context.Context, c domain.Acceptance) (domain.Receipt, error) {
				commits++
				if c.Route.Generation != int64(commits) {
					t.Fatal("route snapshot not refreshed")
				}
				if commits < successAt {
					return domain.Receipt{}, domain.ErrRouteChanged
				}
				return c.Receipt, nil
			}}, funcResolver(func() (domain.RouteSnapshot, error) {
				generation++
				r := route()
				r.Generation = generation
				return r, nil
			}))
			_, err := service.AcceptInbound(context.Background(), input())
			if successAt <= 3 && err != nil {
				t.Fatal(err)
			}
			if successAt > 3 && !errors.Is(err, domain.ErrUnavailable) {
				t.Fatalf("unbounded retries: %v", err)
			}
			if commits > 3 {
				t.Fatalf("commits %d", commits)
			}
		})
	}
}
func funcResolver(f func() (domain.RouteSnapshot, error)) resolveFunc {
	return func(context.Context, string, string) (domain.RouteSnapshot, error) { return f() }
}
func TestInvalidOrUnavailableRouteDoesNotCommit(t *testing.T) {
	for _, mismatch := range []string{"error", "account", "tenant", "digest", "generation"} {
		t.Run(mismatch, func(t *testing.T) {
			service := New(ledgerStub{commit: func(context.Context, domain.Acceptance) (domain.Receipt, error) {
				t.Fatal("invalid route committed")
				return domain.Receipt{}, nil
			}}, funcResolver(func() (domain.RouteSnapshot, error) {
				r := route()
				switch mismatch {
				case "error":
					return r, errors.New("database down")
				case "account":
					r.AccountID = "other"
				case "tenant":
					r.TenantID = ""
				case "digest":
					r.ManifestDigest = "not-a-digest"
				case "generation":
					r.Generation = 0
				}
				return r, nil
			}))
			if _, err := service.AcceptInbound(context.Background(), input()); !errors.Is(err, domain.ErrUnavailable) {
				t.Fatalf("invalid route accepted: %v", err)
			}
		})
	}
}
func TestStopAfterResolvePreventsNewCommit(t *testing.T) {
	var service *Service
	service = New(ledgerStub{commit: func(context.Context, domain.Acceptance) (domain.Receipt, error) {
		t.Fatal("commit started after stop")
		return domain.Receipt{}, nil
	}}, funcResolver(func() (domain.RouteSnapshot, error) { service.Stop(); return route(), nil }))
	if _, err := service.AcceptInbound(context.Background(), input()); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("stop result: %v", err)
	}
}
func TestConcurrentEntryBoundAndReplayPriority(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var calls atomic.Int32
	service, err := NewWithOptions(ledgerStub{find: func(_ context.Context, k domain.EventKey) (domain.Receipt, string, bool, error) {
		if k.EventID == "old" {
			return domain.Receipt{Decision: "ignore"}, strings.Repeat("a", 64), true, nil
		}
		return domain.Receipt{}, "", false, nil
	}, commit: func(_ context.Context, c domain.Acceptance) (domain.Receipt, error) {
		calls.Add(1)
		close(entered)
		<-release
		return c.Receipt, nil
	}}, routes(), Options{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, err := service.AcceptInbound(context.Background(), input()); done <- err }()
	<-entered
	in := input()
	in.Key.EventID = "new"
	if _, err = service.AcceptInbound(context.Background(), in); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("concurrency gate: %v", err)
	}
	in.Key.EventID = "old"
	if _, err = service.AcceptInbound(context.Background(), in); err != nil {
		t.Fatalf("replay blocked by active concurrency gate: %v", err)
	}
	service.Stop()
	in.Key.EventID = "after-stop"
	if _, err = service.AcceptInbound(context.Background(), in); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("stop: %v", err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatalf("in-flight drain: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("commit count=%d", calls.Load())
	}
}
func TestInvalidInputNeverHitsLedger(t *testing.T) {
	service := New(ledgerStub{find: func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error) {
		t.Fatal("invalid input reached ledger")
		return domain.Receipt{}, "", false, nil
	}}, routes())
	in := input()
	in.Kind = "interaction"
	if _, err := service.AcceptInbound(context.Background(), in); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("interaction text accepted: %v", err)
	}
}
func TestDatabaseFailuresAndTimeoutPropagate(t *testing.T) {
	failure := errors.New("commit failure")
	service := New(ledgerStub{commit: func(context.Context, domain.Acceptance) (domain.Receipt, error) { return domain.Receipt{}, failure }}, routes())
	if _, err := service.AcceptInbound(context.Background(), input()); !errors.Is(err, failure) {
		t.Fatalf("commit error lost: %v", err)
	}
	service, err := NewWithOptions(ledgerStub{find: func(ctx context.Context, _ domain.EventKey) (domain.Receipt, string, bool, error) {
		<-ctx.Done()
		return domain.Receipt{}, "", false, ctx.Err()
	}}, routes(), Options{MaxConcurrent: 1, Timeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.AcceptInbound(context.Background(), input()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout lost: %v", err)
	}
}

func TestReceiptLookupsAreBounded(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	service, err := NewWithOptions(ledgerStub{find: func(context.Context, domain.EventKey) (domain.Receipt, string, bool, error) {
		close(entered)
		<-release
		return domain.Receipt{Decision: "ignore"}, strings.Repeat("a", 64), true, nil
	}}, routes(), Options{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, err := service.AcceptInbound(context.Background(), input()); done <- err }()
	<-entered
	if _, err = service.AcceptInbound(context.Background(), input()); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("unbounded receipt lookups: %v", err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
