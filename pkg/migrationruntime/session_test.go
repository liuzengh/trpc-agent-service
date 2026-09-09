package migrationruntime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

func sessionTestKey(t *testing.T, tenantID, sessionID string) session.Key {
	t.Helper()
	appName, err := storage.TenantScopedAppName(&tenant.Tenant{ID: tenantID}, "support")
	if err != nil {
		t.Fatal(err)
	}
	return session.Key{AppName: appName, UserID: "owner:with:separators", SessionID: sessionID}
}

func newRedisSessionBackend(t *testing.T) (*sessionBackend, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	service, err := sessionredis.NewService(sessionredis.WithRedisClientURL("redis://" + server.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	backend := &sessionBackend{tenantID: "tenant-a", service: service, redis: client, options: SessionBackendOptions{MaxEvents: 100, MaxSessions: 100}}
	t.Cleanup(func() { _ = backend.close() })
	return backend, server
}

func TestSessionInventoryFindsPredatingOfficialRedisSessionsAndIsTenantScoped(t *testing.T) {
	backend, server := newRedisSessionBackend(t)
	ctx := context.Background()
	want := []string{}
	for _, id := range []string{"one", "two"} {
		key := sessionTestKey(t, "tenant-a", id)
		if _, err := backend.service.CreateSession(ctx, key, session.StateMap{"stage": []byte("before")}); err != nil {
			t.Fatal(err)
		}
		encoded, _ := sessionRecordKey(key)
		want = append(want, encoded)
	}
	legacy, err := sessionredis.NewService(sessionredis.WithRedisClientURL("redis://"+server.Addr()), sessionredis.WithCompatMode(sessionredis.CompatModeTransition))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	legacyKey := sessionTestKey(t, "tenant-a", "legacy")
	if _, err := legacy.CreateSession(ctx, legacyKey, nil); err != nil {
		t.Fatal(err)
	}
	legacyEncoded, _ := sessionRecordKey(legacyKey)
	want = append(want, legacyEncoded)
	if _, err := backend.service.CreateSession(ctx, sessionTestKey(t, "tenant-a-extra", "foreign"), nil); err != nil {
		t.Fatal(err)
	}
	var got []string
	cursor := ""
	for page := 0; page < 3; page++ {
		keys, next, done, err := backend.Keys(ctx, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, keys...)
		if done {
			break
		}
		if next == cursor {
			t.Fatal("inventory cursor stalled")
		}
		cursor = next
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory=%v want=%v", got, want)
	}
}

func TestSessionBackendRejectsSharedStateTTLAndTruncation(t *testing.T) {
	for _, unsupported := range []string{"app", "user", "ttl", "truncation"} {
		t.Run(unsupported, func(t *testing.T) {
			backend, server := newRedisSessionBackend(t)
			ctx := context.Background()
			key := sessionTestKey(t, "tenant-a", "one")
			value, err := backend.service.CreateSession(ctx, key, nil)
			if err != nil {
				t.Fatal(err)
			}
			switch unsupported {
			case "app":
				err = backend.service.UpdateAppState(ctx, key.AppName, session.StateMap{"shared": []byte("value")})
			case "user":
				err = backend.service.UpdateUserState(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID}, session.StateMap{"shared": []byte("value")})
			case "ttl":
				server.SetTTL("hashidx:meta:"+key.AppName+":{"+key.UserID+"}:"+key.SessionID, time.Hour)
			case "truncation":
				backend.options.MaxEvents = 1
				err = backend.service.AppendEvent(ctx, value, &event.Event{ID: "one", Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "one"}}}}})
			}
			if err != nil {
				t.Fatal(err)
			}
			if unsupported == "truncation" {
				recordKey, _ := sessionRecordKey(key)
				if _, err := backend.Read(ctx, recordKey, 1); err == nil {
					t.Fatal("truncated transcript accepted")
				}
			} else if _, _, _, err := backend.Keys(ctx, "", 10); !errors.Is(err, ErrSessionMigrationUnsupported) {
				t.Fatalf("unsupported capability error=%v", err)
			}
		})
	}
}

func TestSessionBackendReadSupportsUnversionedCapture(t *testing.T) {
	backend, _ := newRedisSessionBackend(t)
	ctx := context.Background()
	key := sessionTestKey(t, "tenant-a", "capture")
	if _, err := backend.service.CreateSession(ctx, key, session.StateMap{"phase": []byte("open")}); err != nil {
		t.Fatal(err)
	}
	keyText, _ := sessionRecordKey(key)
	for _, deleted := range []bool{false, true} {
		if deleted {
			if err := backend.service.DeleteSession(ctx, key); err != nil {
				t.Fatal(err)
			}
		}
		captured, err := backend.Read(ctx, keyText, 0)
		if err != nil {
			t.Fatalf("capture deleted=%v: %v", deleted, err)
		}
		versioned, err := backend.Read(ctx, keyText, 3)
		if err != nil || captured.Version != 0 || captured.Deleted != deleted || captured.Hash != versioned.Hash || !reflect.DeepEqual(captured.Payload, versioned.Payload) {
			t.Fatalf("capture changed canonical state: deleted=%v captured=%+v versioned=%+v err=%v", deleted, captured, versioned, err)
		}
		if err := captured.Validate(); err != nil {
			t.Fatalf("capture is invalid: %v", err)
		}
	}
}

func TestSessionBackendReadApplyCopiesRealPayloadAndTombstone(t *testing.T) {
	source, _ := newRedisSessionBackend(t)
	targetService := inmemory.NewSessionService()
	t.Cleanup(func() { _ = targetService.Close() })
	target := &sessionBackend{tenantID: "tenant-a", service: targetService, options: source.options}
	ctx := context.Background()
	key := sessionTestKey(t, "tenant-a", "one")
	value, err := source.service.CreateSession(ctx, key, session.StateMap{"phase": []byte("open")})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.service.AppendEvent(ctx, value, &event.Event{ID: "one", Timestamp: time.Date(2026, 9, 6, 1, 2, 3, 0, time.UTC), Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "hello"}}}}}); err != nil {
		t.Fatal(err)
	}
	keyText, _ := sessionRecordKey(key)
	record, err := source.Read(ctx, keyText, 7)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := target.Apply(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	got, err := target.Read(ctx, keyText, 7)
	if err != nil || got.Hash != record.Hash {
		t.Fatalf("target read mismatch: %v, hash=%s want=%s", err, got.Hash, record.Hash)
	}
	if err := source.service.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	deleted, err := source.Read(ctx, keyText, 8)
	if err != nil || !deleted.Deleted {
		t.Fatalf("missing source did not become tombstone: %v %+v", err, deleted)
	}
	if err := target.Apply(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if actual, _ := target.service.GetSession(ctx, key); actual != nil {
		t.Fatal("target session survived tombstone")
	}
}

type sessionRouteFixture struct {
	profile string
	active  bool
	writes  [][]string
}

func (r *sessionRouteFixture) WithRead(ctx context.Context, _ string, _ datamigration.Domain, _ string, fn func(context.Context, string) error) error {
	return fn(ctx, r.profile)
}
func (r *sessionRouteFixture) WithWrite(ctx context.Context, _ string, _ datamigration.Domain, _ string, keys []string, fn func(context.Context, string) error) error {
	if r.active && len(keys) == 0 {
		return datamigration.ErrMigrationCapability
	}
	r.writes = append(r.writes, append([]string(nil), keys...))
	return fn(ctx, r.profile)
}

func TestLiveSessionServiceUsesCurrentRouteAndBlocksUncapturableWrites(t *testing.T) {
	ctx := context.Background()
	source, target := inmemory.NewSessionService(), inmemory.NewSessionService()
	t.Cleanup(func() { _ = source.Close(); _ = target.Close() })
	key := sessionTestKey(t, "tenant-a", "one")
	if _, err := target.CreateSession(ctx, key, session.StateMap{"location": []byte("target")}); err != nil {
		t.Fatal(err)
	}
	routes := &sessionRouteFixture{profile: "target", active: true}
	backends := &SessionBackends{profiles: sessionTestProfiles{}, backends: map[string]*sessionBackend{"tenant-a\x00target\x00null": {tenantID: "tenant-a", service: target}}}
	service := &liveSessionService{inner: source, backends: backends, coordinator: routes, tenantID: "tenant-a", fallback: "source"}
	value, err := service.GetSession(ctx, key)
	if err != nil || value == nil || string(value.State["location"]) != "target" {
		t.Fatalf("read current route: value=%v err=%v", value, err)
	}
	if err := service.UpdateSessionState(ctx, key, session.StateMap{"location": []byte("after-cutover")}); err != nil {
		t.Fatal(err)
	}
	if original, _ := source.GetSession(ctx, key); original != nil {
		t.Fatal("cached source received target write")
	}
	if len(routes.writes) != 1 || len(routes.writes[0]) != 1 {
		t.Fatalf("write capture identities=%v", routes.writes)
	}
	if err := service.UpdateAppState(ctx, key.AppName, session.StateMap{"secret": []byte("shared")}); !errors.Is(err, datamigration.ErrMigrationCapability) {
		t.Fatalf("uncapturable write accepted: %v", err)
	}
	state, _ := target.ListAppStates(ctx, key.AppName)
	if len(state) != 0 {
		t.Fatal("rejected shared state write reached backend")
	}
}

type sessionTestProfiles struct{}

func (sessionTestProfiles) ResolveBackendProfile(string) (storage.BackendProfile, error) {
	return storage.BackendProfile{Backend: "postgres"}, nil
}

type sessionRedisTestProfiles struct{ endpoint string }

func (p sessionRedisTestProfiles) ResolveBackendProfile(string) (storage.BackendProfile, error) {
	return storage.BackendProfile{Backend: "redis", ConnectionString: p.endpoint, AllowInsecure: true}, nil
}

func TestSessionBackendCacheEvictsOnlyReleasedClients(t *testing.T) {
	server := miniredis.RunT(t)
	backends := &SessionBackends{profiles: sessionRedisTestProfiles{endpoint: "redis://" + server.Addr()}, options: SessionBackendOptions{MaxBackends: 1, MaxSessions: 100, MaxEvents: 100}, backends: make(map[string]*sessionBackend)}
	t.Cleanup(func() { _ = backends.Close() })
	ctx := context.Background()
	config := tenant.StorageConfig{}
	var previous *sessionBackend
	for index := 0; index < 4; index++ {
		current, err := backends.backend(ctx, fmt.Sprintf("tenant-%d", index), "redis", config)
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil {
			if err := previous.redis.Ping(ctx).Err(); err == nil {
				t.Fatal("evicted backend connection remained open")
			}
		}
		if _, err := backends.backend(ctx, "blocked-tenant", "redis", config); err == nil {
			t.Fatal("active backend was evicted")
		}
		if err := current.redis.Ping(ctx).Err(); err != nil {
			t.Fatalf("active reference closed: %v", err)
		}
		release := backends.releaseBackend(current)
		release()
		release()
		if len(backends.backends) != 1 || current.refs != 0 {
			t.Fatalf("cache state: size=%d refs=%d", len(backends.backends), current.refs)
		}
		previous = current
	}
}
