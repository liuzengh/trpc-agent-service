package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"net/http"
	"testing"
)

func redisMemoryFactoryFixture() (domain.Grant, domain.Plan) {
	g, p := memoryFactoryFixture()
	b := &p.Memory.Backend
	b.Kind = datav1.Redis
	b.Adapter = "managed-redis-v1"
	b.PostgreSQL = nil
	b.Redis = &datav1.RedisTarget{Host: "redis.invalid", Port: 6379, Username: "memory_runtime", Database: 2, TLS: true}
	p.Memory.Credential.AudienceDigest, _ = b.Digest()
	return g, p
}
func TestRedisMemoryFactoryFixedTargetAndClose(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "open_failure"}[fail], func(t *testing.T) {
			g, p := redisMemoryFactoryFixture()
			secret := "redis://attacker.invalid:1234/0?ssl=false"
			f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
				var req resolveRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if len(req.Uses) != 3 || req.Uses[2].AudienceDigest != p.Memory.Credential.AudienceDigest {
					t.Error("wrong closure")
				}
				b := responseFixture(g, p)
				u := p.Memory.Credential
				b.Credentials = append(b.Credentials, credentialWire{CredentialID: u.CredentialID, Purpose: u.Purpose, AudienceDigest: u.AudienceDigest, CredentialRevision: 1, Value: secret})
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(b)
			})
			ss := &fakeStore{}
			ms := &fakeMemoryStore{}
			f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) { return ss, nil }
			f.openMemory = func(context.Context, string, memorystore.Target, int) (memoryStore, error) {
				t.Error("PG fallback invoked")
				return nil, errors.New("wrong adapter")
			}
			f.openRedisMemory = func(ctx context.Context, target memorystore.RedisTarget, password string, capacity int) (memoryStore, error) {
				if target.Host != "redis.invalid" || target.Database != 2 || target.Username != "memory_runtime" || !target.TLS || target.MaxConcurrency != 1 || target.Port != 6379 || password != secret || capacity != 1<<20 {
					t.Error("secret changed immutable Redis target")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("missing timeout")
				}
				if fail {
					return nil, memorystore.ErrUnavailable
				}
				return ms, nil
			}
			rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
			if fail {
				if !errors.Is(err, application.ErrDependency) || rt != nil || !ss.closed {
					t.Fatalf("failed open retained Session: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			p.Memory.Backend.Redis.Host = "changed"
			if rt.(*attempt).plan.Memory.Backend.Redis.Host != "redis.invalid" {
				t.Fatal("mutable target")
			}
			rt.Close()
			rt.Close()
			if !ss.closed || !ms.closed {
				t.Fatal("leak")
			}
		})
	}
}
func TestRedisMemoryPlanRejectBeforeResolve(t *testing.T) {
	for name, mutate := range map[string]func(*domain.Plan){
		"target": func(p *domain.Plan) { p.Memory.Backend.Redis.Host = "changed.invalid" },
		"db":     func(p *domain.Plan) { p.Memory.Backend.Redis.Database = 0 },
		"tls":    func(p *domain.Plan) { p.Memory.Backend.Redis.TLS = false },
		"user": func(p *domain.Plan) {
			p.Memory.Backend.Redis.Username = "default"
			p.Memory.Credential.AudienceDigest, _ = p.Memory.Backend.Digest()
		},
		"nil":  func(p *domain.Plan) { p.Memory.Backend.Redis = nil },
		"role": func(p *domain.Plan) { p.Memory.Backend.Isolation = datav1.SessionIsolation },
	} {
		t.Run(name, func(t *testing.T) {
			g, p := redisMemoryFactoryFixture()
			mutate(&p)
			f, _ := newFixtureFactory(t, func(http.ResponseWriter, *http.Request) { t.Error("resolved invalid plan") })
			if rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil }); rt != nil || !errors.Is(err, application.ErrManifestInvalid) {
				t.Fatalf("%v %v", rt, err)
			}
		})
	}
}
