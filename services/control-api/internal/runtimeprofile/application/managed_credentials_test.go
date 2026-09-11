package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"reflect"
	"testing"
)

type memoryCredentialTargets struct{ offline bool }

func (*memoryCredentialTargets) CheckBackend(context.Context, string, string, uint64, string) error {
	return nil
}
func (r *memoryCredentialTargets) ResolveManagedCredentialAudience(_ context.Context, tenant, id string, revision uint64, role string) (string, error) {
	if r.offline || tenant != "tnt_a" || !((role == "memory" && (id == "memory-pg" || id == "memory-redis")) || (role == "session" && id == "session-redis")) {
		return "", errors.New("target unavailable")
	}
	return memoryTarget(tenant, id, revision).Digest()
}
func memoryPGTarget(tenant, id string, revision uint64) datav1.Snapshot {
	return datav1.Snapshot{SchemaVersion: "v1", TenantID: tenant, BackendID: id, BackendRevision: revision, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: datav1.MemoryIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, PostgreSQL: &datav1.PostgresTarget{Host: "pg.internal", Port: 5432, Database: "memory", Username: "memory_runtime", SSLMode: "verify-full"}}
}
func memoryTarget(tenant, id string, revision uint64) datav1.Snapshot {
	s := memoryPGTarget(tenant, id, revision)
	if id == "memory-redis" || id == "session-redis" {
		s.Kind = datav1.Redis
		s.Adapter = "managed-redis-v1"
		s.PostgreSQL = nil
		s.Redis = &datav1.RedisTarget{Host: "redis.internal", Port: 6379, Database: 3, Username: "memory_runtime", TLS: true}
	}
	if id == "session-redis" {
		s.Isolation = datav1.SessionIsolation
		s.Redis.Username = "session_runtime"
	}
	return s
}
func memoryCredentialCommand() application.SaveCredentialDraftCommand {
	c := modelCredentialCommand(1, "seed-memory", "model-password")
	c.Write.Config.Storage = map[string]application.StorageConfig{"memory": {Kind: domain.StorageKindManagedMemory, BackendID: "memory-pg", BackendRevision: 1}}
	password := "memory-private-password"
	c.Write.Credentials["storage"] = map[string]map[string]application.CredentialAction{"memory": {"dsn_password": {Action: "replace", Value: &password}}}
	return c
}
func TestManagedPasswordSaveResolveAndDirectoryIndependentRotation(t *testing.T) {
	for _, backendID := range []string{"memory-pg", "memory-redis", "session-redis"} {
		t.Run(backendID, func(t *testing.T) {
			targets := &memoryCredentialTargets{}
			h := newCredentialHarnessWithBackend(t, targets, targets)
			command := memoryCredentialCommand()
			role := "memory"
			if backendID == "session-redis" {
				role = "session"
			}

			r := command.Write.Config.Storage["memory"]
			r.BackendID = backendID
			if role == "session" {
				r.Kind = domain.StorageKindManagedSession
				delete(command.Write.Config.Storage, "memory")
				action := command.Write.Credentials["storage"]["memory"]
				delete(command.Write.Credentials["storage"], "memory")
				command.Write.Credentials["storage"][role] = action
			}
			command.Write.Config.Storage[role] = r
			h.save(t, command)
			spec := h.spec(t)
			memory := spec.Storage[role]
			digest, _ := memoryTarget("tnt_a", backendID, 1).Digest()
			if memory.DSNCredentialID == "" || memory.CredentialAudienceDigest != digest {
				t.Fatal("canonical binding absent")
			}
			record := h.store.records[memory.DSNCredentialID]
			if record.Purpose != "dsn_password" || record.AudienceDigest != digest || h.decrypt(t, record) != "memory-private-password" {
				t.Fatal("wrong credential record")
			}
			revision := publishCredentialHarness(t, h)
			read, err := h.service.GetCredentialRevision(context.Background(), "tnt_a", "rpf_a", "usr_owner", 1)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(read)
			for _, secret := range []string{memory.DSNCredentialID, digest, "memory-private-password", "credential_audience_digest", "pg.internal"} {
				if bytes.Contains(raw, []byte(secret)) {
					t.Fatal("public leak", secret)
				}
			}
			state := read.CredentialStates["storage"][role]["dsn_password"]
			if !state.Configured || state.AssociationToken == "" {
				t.Fatal("missing public state")
			}
			use := application.CredentialUse{CredentialID: memory.DSNCredentialID, Purpose: "dsn_password", AudienceDigest: digest}
			if err = h.service.CheckUsable(context.Background(), application.CheckProfileCredentialsCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ProfileRevisionNumber: 1, Uses: []application.CredentialUse{use}}); err != nil {
				t.Fatal(err)
			}
			ownerBatch, err := h.service.ResolveStorageForOwner(context.Background(), application.CheckProfileCredentialsCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ProfileRevisionNumber: 1, Uses: []application.CredentialUse{use}})
			if err != nil || len(ownerBatch.Credentials) != 1 || string(ownerBatch.Credentials[0].Value) != "memory-private-password" {
				t.Fatal("owner storage resolve", err)
			}
			ownerBatch.Clear()
			if denied, err := h.service.ResolveStorageForOwner(context.Background(), application.CheckProfileCredentialsCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_member", ProfileRevisionNumber: 1, Uses: []application.CredentialUse{use}}); err == nil || len(denied.Credentials) != 0 {
				t.Fatal("member resolved storage credential")
			}
			request, verifier := executionForHarness(h, []application.CredentialUse{use})
			consumer := credentialConsumerService(h, verifier, h.cipher)
			batch, err := consumer.ResolveForAttempt(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.Credentials) != 1 || string(batch.Credentials[0].Value) != "memory-private-password" {
				t.Fatal("resolve value")
			}
			batch.Clear()
			// Immutable Revision plus association token, not current directory, controls rotation.
			targets.offline = true
			value := "rotated-memory-password"
			update := application.UpdateUsedCredentialCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", IdempotencyKey: "rotate-memory", Update: application.CredentialUpdate{Target: application.PublishedCredentialTarget{ProfileRevisionNumber: 1, Category: "storage", ResourceName: role, PurposeField: "dsn_password", AssociationToken: state.AssociationToken}, Action: "replace", ExpectedCredentialRevision: 1, Value: &value}}
			if _, err = h.service.UpdateUsedProfileCredential(context.Background(), update); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(revision, h.store.base.revisions[resourceKey("tnt_a", "rpf_a")][0]) {
				t.Fatal("rotation changed revision")
			}
			batch, err = consumer.ResolveForAttempt(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			defer batch.Clear()
			if string(batch.Credentials[0].Value) != value {
				t.Fatal("rotation not resolved")
			}
		})
	}
}
func TestManagedPGPasswordBackendChangeRequiresReplace(t *testing.T) {
	targets := &memoryCredentialTargets{}
	h := newCredentialHarnessWithBackend(t, targets, targets)
	c := memoryCredentialCommand()
	h.save(t, c)
	c.Write.ExpectedDraftRevision = 2
	c.IdempotencyKey = "change-keep"
	c.Write.Credentials = application.CredentialActions{}
	r := c.Write.Config.Storage["memory"]
	r.BackendRevision = 2
	c.Write.Config.Storage["memory"] = r
	if _, err := h.service.SaveCredentialDraft(context.Background(), c); !errors.Is(err, domain.ErrCredentialAssociation) {
		t.Fatal("keep rebound password", err)
	}
	password := "replacement"
	c.IdempotencyKey = "change-replace"
	c.Write.Credentials = application.CredentialActions{"storage": {"memory": {"dsn_password": {Action: "replace", Value: &password}}}}
	h.save(t, c)
	digest, _ := memoryPGTarget("tnt_a", "memory-pg", 2).Digest()
	if h.spec(t).Storage["memory"].CredentialAudienceDigest != digest {
		t.Fatal("replacement not rebound")
	}
	c.Write.ExpectedDraftRevision = 3
	c.IdempotencyKey = "clear-memory"
	c.Write.Credentials["storage"]["memory"]["dsn_password"] = application.CredentialAction{Action: "clear"}
	targets.offline = true
	h.save(t, c)
	if r := h.spec(t).Storage["memory"]; r.DSNCredentialID != "" || r.CredentialAudienceDigest != "" {
		t.Fatal("clear retained binding")
	}
}
func TestManagedPGPasswordRejectsMissingOrWrongTarget(t *testing.T) {
	targets := &memoryCredentialTargets{}
	for _, mode := range []string{"nil", "unknown", "offline"} {
		t.Run(mode, func(t *testing.T) {
			targets.offline = mode == "offline"
			var resolver application.ManagedCredentialTargetResolver = targets
			if mode == "nil" {
				resolver = nil
			}
			h := newCredentialHarnessWithBackend(t, targets, resolver)
			c := memoryCredentialCommand()
			if mode == "unknown" {
				r := c.Write.Config.Storage["memory"]
				r.BackendID = "unknown"
				c.Write.Config.Storage["memory"] = r
			}
			if _, err := h.service.SaveCredentialDraft(context.Background(), c); err == nil {
				t.Fatal("untrusted target accepted")
			}
		})
	}
}

func TestManagedMemoryCrossBackendKeepCannotRebind(t *testing.T) {
	for _, ids := range [][2]string{{"memory-pg", "memory-redis"}, {"memory-redis", "memory-pg"}} {
		t.Run(ids[0]+"_to_"+ids[1], func(t *testing.T) {
			targets := &memoryCredentialTargets{}
			h := newCredentialHarnessWithBackend(t, targets, targets)
			c := memoryCredentialCommand()
			r := c.Write.Config.Storage["memory"]
			r.BackendID = ids[0]
			c.Write.Config.Storage["memory"] = r
			h.save(t, c)
			old := h.spec(t).Storage["memory"]
			c.Write.ExpectedDraftRevision = 2
			c.IdempotencyKey = "cross-keep"
			r.BackendID = ids[1]
			c.Write.Config.Storage["memory"] = r
			c.Write.Credentials = application.CredentialActions{}
			if _, err := h.service.SaveCredentialDraft(context.Background(), c); !errors.Is(err, domain.ErrCredentialAssociation) {
				t.Fatal("cross-backend keep accepted", err)
			}
			if h.spec(t).Storage["memory"] != old {
				t.Fatal("failed keep changed binding")
			}
			password := "new-backend-password"
			c.IdempotencyKey = "cross-replace"
			c.Write.Credentials = application.CredentialActions{"storage": {"memory": {"dsn_password": {Action: "replace", Value: &password}}}}
			h.save(t, c)
			next := h.spec(t).Storage["memory"]
			want, _ := memoryTarget("tnt_a", ids[1], 1).Digest()
			if next.CredentialAudienceDigest != want || next.DSNCredentialID == old.DSNCredentialID {
				t.Fatal("replace did not rebind")
			}
		})
	}
}

func TestManagedRedisSessionPasswordBackendChangeRequiresReplace(t *testing.T) {
	targets := &memoryCredentialTargets{}
	h := newCredentialHarnessWithBackend(t, targets, targets)
	c := memoryCredentialCommand()
	r0 := c.Write.Config.Storage["memory"]
	r0.Kind = domain.StorageKindManagedSession
	r0.BackendID = "session-redis"
	c.Write.Config.Storage = map[string]application.StorageConfig{"session": r0}
	c.Write.Credentials["storage"] = map[string]map[string]application.CredentialAction{"session": c.Write.Credentials["storage"]["memory"]}
	h.save(t, c)
	c.Write.ExpectedDraftRevision = 2
	c.IdempotencyKey = "change-keep"
	c.Write.Credentials = application.CredentialActions{}
	r := c.Write.Config.Storage["session"]
	r.BackendRevision = 2
	c.Write.Config.Storage["session"] = r
	if _, err := h.service.SaveCredentialDraft(context.Background(), c); !errors.Is(err, domain.ErrCredentialAssociation) {
		t.Fatal("keep rebound password", err)
	}
	password := "replacement"
	c.IdempotencyKey = "change-replace"
	c.Write.Credentials = application.CredentialActions{"storage": {"session": {"dsn_password": {Action: "replace", Value: &password}}}}
	h.save(t, c)
	digest, _ := memoryTarget("tnt_a", "session-redis", 2).Digest()
	if h.spec(t).Storage["session"].CredentialAudienceDigest != digest {
		t.Fatal("replacement not rebound")
	}
	c.Write.ExpectedDraftRevision = 3
	c.IdempotencyKey = "clear-memory"
	c.Write.Credentials["storage"]["session"]["dsn_password"] = application.CredentialAction{Action: "clear"}
	targets.offline = true
	h.save(t, c)
	if r := h.spec(t).Storage["session"]; r.DSNCredentialID != "" || r.CredentialAudienceDigest != "" {
		t.Fatal("clear retained binding")
	}
}
