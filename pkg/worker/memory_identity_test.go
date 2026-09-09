package worker

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// Synthetic credentials and bodies only. This test crosses real public protocol
// adapters and the production actor-scoped Memory wrapper; it does not contact
// a provider or database and it does not run a model.
func TestAuthenticatedChannelsDoNotAliasMemoryActors(t *testing.T) {
	const rawUser = "12345"
	parse := func(t *testing.T, kind, account, corp string) (*channel.InboundMessage, channel.SessionIdentity) {
		t.Helper()
		binding := &tenant.ChannelBinding{Type: kind, AccountID: account, AgentApp: "support", AppID: "7", Token: "testtokenforproof", Secret: "synthetic-webhook-secret-at-least-32-characters", AccessPolicy: tenant.ChannelAccessPolicy{AllowDirectMessages: true, AllowedUsers: []string{rawUser}}}
		var adapter channel.Adapter
		var req = httptest.NewRequest("POST", "/webhook", nil)
		if kind == "telegram" {
			adapter = channel.NewTelegramAdapter()
			body := fmt.Sprintf(`{"update_id":11,"message":{"message_id":9,"from":{"id":12345},"chat":{"id":12345,"type":"private"},"date":%d,"text":"synthetic namespace test"}}`, time.Now().Unix())
			req = httptest.NewRequest("POST", "/webhook", strings.NewReader(body))
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", binding.Secret)
		} else {
			adapter = channel.NewWeWorkAdapter()
			key := []byte("0123456789abcdef0123456789abcdef")
			binding.EncodingAESKey = strings.TrimSuffix(base64.StdEncoding.EncodeToString(key), "=")
			binding.Config = map[string]string{"corp_id": corp}
			xml := []byte(fmt.Sprintf(`<xml><FromUserName>12345</FromUserName><CreateTime>%d</CreateTime><MsgType>text</MsgType><Content>synthetic namespace test</Content><MsgId>42</MsgId><AgentID>7</AgentID></xml>`, time.Now().Unix()))
			plain := bytes.Repeat([]byte{0x42}, 16)
			size := make([]byte, 4)
			binary.BigEndian.PutUint32(size, uint32(len(xml)))
			plain = append(plain, size...)
			plain = append(plain, xml...)
			plain = append(plain, []byte(corp)...)
			padding := 32 - len(plain)%32
			plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
			block, err := aes.NewCipher(key)
			if err != nil {
				t.Fatal(err)
			}
			encryptedBytes := make([]byte, len(plain))
			cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encryptedBytes, plain)
			encrypted := base64.StdEncoding.EncodeToString(encryptedBytes)
			stamp := strconv.FormatInt(time.Now().Unix(), 10)
			nonce := "syntheticnonce"
			parts := []string{binding.Token, stamp, nonce, encrypted}
			sort.Strings(parts)
			signature := sha1.Sum([]byte(strings.Join(parts, "")))
			q := url.Values{"timestamp": {stamp}, "nonce": {nonce}, "msg_signature": {hex.EncodeToString(signature[:])}}
			req = httptest.NewRequest("POST", "/webhook?"+q.Encode(), strings.NewReader("<xml><Encrypt>"+encrypted+"</Encrypt></xml>"))
		}
		if err := adapter.VerifySignature(req, binding); err != nil {
			t.Fatalf("%s signature: %v", kind, err)
		}
		msg, err := adapter.ParseInbound(req, binding)
		if err != nil {
			t.Fatalf("%s parse: %v", kind, err)
		}
		// Gateway overwrites these routing fields from the selected binding.
		msg.TenantID = "tenant-proof"
		msg.ChannelAccountID = account
		if err := channel.AuthorizeInbound(binding, msg); err != nil {
			t.Fatalf("%s authorization: %v", kind, err)
		}
		identity, err := channel.BuildSessionIdentity(msg)
		if err != nil {
			t.Fatal(err)
		}
		return msg, identity
	}
	for _, scenario := range []struct{ name, kindB, accountB, corpB string }{
		{"cross-provider", "telegram", "bot-second", ""},
		{"same-provider-different-corporations", "wework", "corp-second-app", "corp-second"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			a, ia := parse(t, "wework", "corp-first-app", "corp-first")
			b, ib := parse(t, scenario.kindB, scenario.accountB, scenario.corpB)
			if ia.SessionID == ib.SessionID {
				t.Fatal("expected channel/account isolated sessions")
			}
			scopedApp, err := storage.TenantScopedAppName(&tenant.Tenant{ID: "tenant-proof"}, "support")
			if err != nil {
				t.Fatal(err)
			}
			base := inmemory.NewMemoryService()
			defer base.Close()
			fenced, err := storage.NewStrictFencedMemoryService(base, memoryIdentityAuthorizer{}, "tenant-proof")
			if err != nil {
				t.Fatal(err)
			}
			scoped, err := newActorScopedMemoryService(fenced, scopedApp)
			if err != nil {
				t.Fatal(err)
			}
			ca := memoryIdentityContext(t, a, ia, scopedApp)
			cb := memoryIdentityContext(t, b, ib, scopedApp)
			marker := "private-marker-for-first-corporation-user"
			if err := scoped.AddMemory(ca, memory.UserKey{AppName: scopedApp, UserID: ia.SessionOwnerID}, marker, nil); err != nil {
				t.Fatal(err)
			}
			got, err := scoped.ReadMemories(cb, memory.UserKey{AppName: scopedApp, UserID: ib.SessionOwnerID}, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("another provider identity read private Memory: %#v", got)
			}
			own, err := scoped.ReadMemories(ca, memory.UserKey{AppName: scopedApp, UserID: ia.SessionOwnerID}, 10)
			if err != nil || len(own) != 1 || own[0].Memory.Memory != marker {
				t.Fatalf("own Memory unavailable: %v %#v", err, own)
			}
			if _, err := fenced.ReadMemories(cb, memory.UserKey{AppName: scopedApp, UserID: own[0].UserID}, 10); !errors.Is(err, fence.ErrScopeMismatch) {
				t.Fatalf("strict fence allowed foreign actor: %v", err)
			}
		})
	}
}

type memoryIdentityAuthorizer struct{}

func (memoryIdentityAuthorizer) Acquire(_ context.Context, token fence.Token) (func() error, error) {
	if err := token.Validate(); err != nil {
		return nil, err
	}
	return func() error { return nil }, nil
}
func memoryIdentityContext(t *testing.T, msg *channel.InboundMessage, id channel.SessionIdentity, app string) context.Context {
	t.Helper()
	ctx := fence.WithToken(context.Background(), fence.Token{TenantID: msg.TenantID, AgentAppID: "app-proof", AgentAppName: "support", ScopedAppName: app, UserID: msg.ExternalUserID, SessionOwnerID: id.SessionOwnerID, SessionID: id.SessionID, ExecutionID: 1, Generation: 1, Value: "proof-execution"})
	ctx, err := contextWithMemoryActor(ctx, msg.TenantID, &Request{TenantID: msg.TenantID, ChannelType: msg.ChannelType, ChannelAccountID: msg.ChannelAccountID, UserID: msg.ExternalUserID, SessionOwnerID: id.SessionOwnerID, SessionID: id.SessionID, IsGroupChat: msg.IsGroupChat}, true)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestProviderMemoryActorReusesGroupsWithoutClaimingLegacyRecords(t *testing.T) {
	const tenantID = "tenant-proof"
	app, err := storage.TenantScopedAppName(&tenant.Tenant{ID: tenantID}, "support")
	if err != nil {
		t.Fatal(err)
	}
	base := &recordingMemoryService{Service: inmemory.NewMemoryService()}
	defer base.Close()
	fenced, err := storage.NewStrictFencedMemoryService(base, memoryIdentityAuthorizer{}, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := newActorScopedMemoryService(fenced, app)
	if err != nil {
		t.Fatal(err)
	}
	contexts := make([]context.Context, 3)
	identities := make([]channel.SessionIdentity, 3)
	for i, conversation := range []string{"direct-user", "group-a", "group-b"} {
		msg := &channel.InboundMessage{TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot-a", ExternalUserID: "12345", ConversationID: conversation, IsGroupChat: i > 0}
		identities[i], err = channel.BuildSessionIdentity(msg)
		if err != nil {
			t.Fatal(err)
		}
		contexts[i] = memoryIdentityContext(t, msg, identities[i], app)
	}
	// Historical raw IDs remain intact but are not automatically assigned to
	// any provider account, even when only one account happens to be configured.
	if err := base.AddMemory(context.Background(), memory.UserKey{AppName: app, UserID: "12345"}, "unattributed historical record", nil); err != nil {
		t.Fatal(err)
	}
	for i, ctx := range contexts {
		entries, err := scoped.ReadMemories(ctx, memory.UserKey{AppName: app, UserID: identities[i].SessionOwnerID}, 10)
		if err != nil || len(entries) != 0 {
			t.Fatalf("legacy raw record aliased for scope %d: entries=%v err=%v", i, entries, err)
		}
	}
	if err := scoped.AddMemory(contexts[1], memory.UserKey{AppName: app, UserID: identities[1].SessionOwnerID}, "account-scoped personal memory", nil); err != nil {
		t.Fatal(err)
	}
	for i, ctx := range contexts {
		entries, err := scoped.ReadMemories(ctx, memory.UserKey{AppName: app, UserID: identities[i].SessionOwnerID}, 10)
		if err != nil || len(entries) != 1 || entries[0].Memory.Memory != "account-scoped personal memory" {
			t.Fatalf("own cross-session recall %d: entries=%v err=%v", i, entries, err)
		}
	}
	sess := session.NewSession(app, identities[0].SessionOwnerID, identities[0].SessionID)
	if err := scoped.EnqueueAutoMemoryJob(contexts[0], sess); err != nil {
		t.Fatal(err)
	}
	actor, _ := channel.MemoryActorID(tenantID, "telegram", "bot-a", "12345")
	if len(base.enqueued) != 1 || base.enqueued[0].UserID != actor || sess.UserID != "12345" {
		t.Fatalf("direct extraction changed Session owner or missed Memory identity: %#v", base.enqueued)
	}
	groupSession := session.NewSession(app, identities[1].SessionOwnerID, identities[1].SessionID)
	if err := scoped.EnqueueAutoMemoryJob(contexts[1], groupSession); err != nil {
		t.Fatal(err)
	}
	if len(base.enqueued) != 1 {
		t.Fatal("group transcript leaked into personal extraction")
	}
	sessions, err := storage.NewStrictFencedSessionService(sessioninmemory.NewSessionService(), memoryIdentityAuthorizer{}, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	defer sessions.Close()
	if _, err := sessions.CreateSession(contexts[0], session.Key{AppName: app, UserID: sess.UserID, SessionID: sess.ID}, nil); err != nil {
		t.Fatalf("unchanged Session owner rejected: %v", err)
	}
	if _, err := sessions.GetSession(contexts[0], session.Key{AppName: app, UserID: actor, SessionID: sess.ID}); !errors.Is(err, fence.ErrScopeMismatch) {
		t.Fatalf("Memory identity accepted as Session owner: %v", err)
	}
	if err := scoped.ClearMemories(contexts[2], memory.UserKey{AppName: app, UserID: identities[2].SessionOwnerID}); err != nil {
		t.Fatal(err)
	}
	historical, err := base.ReadMemories(context.Background(), memory.UserKey{AppName: app, UserID: "12345"}, 10)
	if err != nil || len(historical) != 1 {
		t.Fatalf("legacy record was changed: %v %v", historical, err)
	}
}

func TestContextWithMemoryActorRejectsPartialOrMismatchedAuthority(t *testing.T) {
	req := &Request{TenantID: "tenant-proof", ChannelType: "telegram", ChannelAccountID: "bot-a", UserID: "12345", SessionOwnerID: "12345", SessionID: "session-a"}
	for _, field := range []string{"channel", "account"} {
		copy := *req
		if field == "channel" {
			copy.ChannelType = ""
		} else {
			copy.ChannelAccountID = ""
		}
		if _, err := contextWithMemoryActor(context.Background(), req.TenantID, &copy, true); !errors.Is(err, ErrActorIdentityRequired) {
			t.Fatalf("missing %s: %v", field, err)
		}
	}
	for _, token := range []fence.Token{
		{TenantID: "other-tenant", UserID: req.UserID},
		{TenantID: req.TenantID, UserID: "other-user"},
		{TenantID: req.TenantID, UserID: req.UserID, MemoryUserID: "other-account-actor"},
	} {
		token.AgentAppID = "app-a"
		token.SessionID = req.SessionID
		token.ExecutionID = 1
		token.Generation = 1
		token.Value = "execution"
		if _, err := contextWithMemoryActor(fence.WithToken(context.Background(), token), req.TenantID, req, true); !errors.Is(err, fence.ErrScopeMismatch) {
			t.Fatalf("mismatched authority allowed: %v", err)
		}
	}
}

func TestWorkerMemoryActorRejectionDoesNotAcquireSessionLease(t *testing.T) {
	runtime := &scriptedRunner{}
	w := newBudgetProcessWorker(t, runtime, &recordingBudgetController{})
	req := &Request{TenantID: w.tenant.ID, ChannelType: "telegram", ChannelAccountID: "bot-a", UserID: "12345", SessionID: "session-a", Content: "hello"}
	ctx := fence.WithToken(context.Background(), fence.Token{
		TenantID: w.tenant.ID, AgentAppID: "app-a", UserID: req.UserID,
		MemoryUserID: "another-account", SessionID: req.SessionID,
		ExecutionID: 1, Generation: 1, Value: "execution",
	})
	if _, err := w.Process(ctx, req); !errors.Is(err, fence.ErrScopeMismatch) || !errors.Is(err, ErrExecutionPreflightPermanent) {
		t.Fatalf("actor rejection=%v", err)
	}
	lease, err := w.sessionLocks.AcquireLease(context.Background(), sessionLeaseKey(w.tenant.ID, w.appName, req.UserID, req.SessionID), storage.DefaultLockTTL)
	if err != nil {
		t.Fatalf("rejected actor stranded Session lease: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
