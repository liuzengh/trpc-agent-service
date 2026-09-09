package web_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// fakeBindingChannel implements channels.Channel + channels.BindingAware,
// recording the credentials the dispatcher handed it.
type fakeBindingChannel struct {
	gotCreds channels.BindingCredentials
	called   bool
}

func (f *fakeBindingChannel) Name() string { return "fake" }
func (f *fakeBindingChannel) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {
}
func (f *fakeBindingChannel) Send(_ context.Context, _ channels.OutboundMessage) error {
	return nil
}

func (f *fakeBindingChannel) CallbackHandler(_ channels.Handler, creds channels.BindingCredentials) (http.HandlerFunc, error) {
	if creds.TokenRef == "" {
		// A binding-scoped callback must carry its own credential refs.
		return nil, errors.New("binding lacks callback credentials")
	}
	if creds.TokenRef == "unresolvable" {
		return nil, errors.New("resolve token")
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		f.called = true
		f.gotCreds = creds
	}, nil
}

// fakeBindings implements web.BindingLookup from a map.
type fakeBindings map[string]tenant.ChannelBinding

func (f fakeBindings) BindingByID(_ context.Context, id string) (tenant.ChannelBinding, error) {
	b, ok := f[id]
	if !ok {
		return tenant.ChannelBinding{}, errors.New("unknown binding")
	}
	return b, nil
}

func TestBindingDispatcher(t *testing.T) {
	ch := &fakeBindingChannel{}
	bindings := fakeBindings{
		"b1": {ID: "b1", TenantID: "t1", Channel: "fake", AppID: "a1",
			WebhookPath: "/callback/fake/b1", TokenRef: "tok-b1", AESKeyRef: "aes-b1", Status: "active"},
		"b2": {ID: "b2", TenantID: "t2", Channel: "other", AppID: "a2",
			WebhookPath: "/callback/other/b2", TokenRef: "tok-b2", AESKeyRef: "aes-b2", Status: "active"},
		"b3": {ID: "b3", TenantID: "t3", Channel: "fake", AppID: "a3",
			WebhookPath: "/callback/fake/b3", TokenRef: "unresolvable", AESKeyRef: "aes-b3", Status: "active"},
		"b4": {ID: "b4", TenantID: "t4", Channel: "fake", AppID: "a4",
			WebhookPath: "/callback/fake/b4", Status: "active"},
	}
	d := web.BindingDispatcher{
		Channels: map[string]channels.Channel{"fake": ch},
		Bindings: bindings,
		Handler: channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
			return channels.OutboundMessage{}, nil
		}),
	}
	do := func(channel, binding string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/callback/"+channel+"/"+binding, nil)
		// Direct dispatch bypasses ServeMux, so the path values are set the
		// way the mux would populate them.
		req.SetPathValue("channel", channel)
		req.SetPathValue("binding", binding)
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, req)
		return rec
	}

	if rec := do("unknown", "b1"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown channel must 404, got %d", rec.Code)
	}
	if rec := do("fake", "missing"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown binding must 404, got %d", rec.Code)
	}
	if rec := do("fake", "b2"); rec.Code != http.StatusNotFound {
		t.Fatalf("binding of another channel must 404, got %d", rec.Code)
	}
	if rec := do("fake", "b3"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unresolvable credentials must 503 so the IM redelivers, got %d", rec.Code)
	}
	if rec := do("fake", "b4"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("binding without credential refs must 503, not fall back to the env keys, got %d", rec.Code)
	}

	rec := do("fake", "b1")
	if rec.Code != http.StatusOK || !ch.called {
		t.Fatalf("happy path must dispatch: status=%d called=%v", rec.Code, ch.called)
	}
	if ch.gotCreds.BindingID != "b1" || ch.gotCreds.TokenRef != "tok-b1" || ch.gotCreds.AESKeyRef != "aes-b1" {
		t.Fatalf("adapter must receive the binding's own refs, got %+v", ch.gotCreds)
	}

	// Fail closed without a binding lookup (PG down at startup): 503, never a
	// silent bypass of tenant routing.
	empty := web.BindingDispatcher{Channels: map[string]channels.Channel{"fake": ch}, Handler: d.Handler}
	req := httptest.NewRequest(http.MethodPost, "/callback/fake/b1", nil)
	req.SetPathValue("channel", "fake")
	req.SetPathValue("binding", "b1")
	rec = httptest.NewRecorder()
	empty.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil lookup must fail closed 503, got %d", rec.Code)
	}
}

// Binding config may override the crypt receiver id: two tenants on one
// channel can belong to different IM corps.
func TestBindingCorpIDOverride(t *testing.T) {
	ch := &fakeBindingChannel{}
	bindings := fakeBindings{
		"b1": {ID: "b1", TenantID: "t1", Channel: "fake", AppID: "a1",
			WebhookPath: "/callback/fake/b1", TokenRef: "tok-b1", AESKeyRef: "aes-b1",
			Config: []byte(`{"corp_id":"ww999"}`), Status: "active"},
	}
	d := web.BindingDispatcher{
		Channels: map[string]channels.Channel{"fake": ch},
		Bindings: bindings,
		Handler: channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
			return channels.OutboundMessage{}, nil
		}),
	}
	req := httptest.NewRequest(http.MethodPost, "/callback/fake/b1", nil)
	req.SetPathValue("channel", "fake")
	req.SetPathValue("binding", "b1")
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !ch.called {
		t.Fatalf("dispatch failed: status=%d called=%v", rec.Code, ch.called)
	}
	if ch.gotCreds.CorpID != "ww999" {
		t.Fatalf("corp override must reach the adapter, got %q", ch.gotCreds.CorpID)
	}
}

// Two tenants on one channel share the mock adapter and one gateway, yet each
// callback is routed to its own tenant/app/binding. Redis-gated: the tail of
// the pipeline (dedup + stream) is the real one.
func TestTwoTenantCallbackRouting(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	// Two tenants, two apps, two bindings on the same "mock" channel.
	b1 := tenant.ChannelBinding{ID: "b1", TenantID: "t-1", Channel: "mock", AppID: "app-1",
		WebhookPath: "/callback/mock/b1", Status: tenant.StatusActive}
	b2 := tenant.ChannelBinding{ID: "b2", TenantID: "t-2", Channel: "mock", AppID: "app-2",
		WebhookPath: "/callback/mock/b2", Status: tenant.StatusActive}
	resolver := tenant.NewResolver(fakeStore{data: tenant.Data{
		Tenants: []tenant.Tenant{
			{ID: "t-1", Name: "one", Status: tenant.StatusActive},
			{ID: "t-2", Name: "two", Status: tenant.StatusActive},
		},
		Apps: []tenant.AgentApp{
			{ID: "app-1", TenantID: "t-1", Status: "published"},
			{ID: "app-2", TenantID: "t-2", Status: "published"},
		},
		Bindings: []tenant.ChannelBinding{b1, b2},
	}})

	stream := storage.NewStream(rdb)
	inbound := fmt.Sprintf("test:inbound:%d", time.Now().UnixNano())
	enqueue := web.EnqueueHandler{Stream: stream, Routes: resolver, InStream: inbound}
	for _, gs := range [][2]string{{inbound, "workers"}} {
		if err := stream.EnsureGroup(ctx, gs[0], gs[1]); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = rdb.Del(ctx, inbound) })

	dispatch := web.BindingDispatcher{
		Channels: map[string]channels.Channel{"mock": mock.New()},
		Bindings: resolver,
		Handler:  enqueue,
	}

	post := func(binding string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/callback/mock/"+binding,
			strings.NewReader(`{"msg_id":"m-`+binding+`","user_id":"u1","text":"hi"}`))
		req.SetPathValue("channel", "mock")
		req.SetPathValue("binding", binding)
		rec := httptest.NewRecorder()
		dispatch.ServeHTTP(rec, req)
		return rec
	}
	for _, binding := range []string{"b1", "b2"} {
		rec := post(binding)
		if rec.Code != http.StatusOK {
			t.Fatalf("binding %s callback status = %d body %s", binding, rec.Code, rec.Body.String())
		}
	}

	// Read the stream back: each message must carry its own tenant/app/binding.
	msgs, err := stream.Read(ctx, inbound, "workers", "test-reader", 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][2]string{} // binding → {tenant, app}
	for _, m := range msgs {
		var in channels.InboundMessage
		if err := json.Unmarshal(m.Payload, &in); err != nil {
			t.Fatal(err)
		}
		got[in.BindingID] = [2]string{in.TenantID, in.AppID}
		_ = stream.Ack(ctx, inbound, "workers", m.ID)
	}
	if got["b1"] != [2]string{"t-1", "app-1"} || got["b2"] != [2]string{"t-2", "app-2"} {
		t.Fatalf("per-tenant routing broken: %+v", got)
	}
}

// legacyChannel implements channels.Channel without BindingAware: it keeps
// serving its env-configured callback path only, so a binding-style dispatch
// is a 404 rather than a silent fallback onto the env credentials.
type legacyChannel struct{}

func (legacyChannel) Name() string                                        { return "legacy" }
func (legacyChannel) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {}
func (legacyChannel) Send(_ context.Context, _ channels.OutboundMessage) error {
	return nil
}

func TestBindingDispatcherLegacyChannel(t *testing.T) {
	ch := &fakeBindingChannel{}
	d := web.BindingDispatcher{
		Channels: map[string]channels.Channel{
			"legacy": legacyChannel{},
			"fake":   ch,
		},
		Bindings: fakeBindings{
			// Malformed config jsonb: the corp-id override must degrade to
			// empty (with a warning), never fail the dispatch.
			"b1": {ID: "b1", TenantID: "t1", Channel: "fake", AppID: "a1",
				WebhookPath: "/callback/fake/b1", TokenRef: "tok-b1",
				Config: []byte(`{not-json`), Status: "active"},
		},
		Handler: channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
			return channels.OutboundMessage{}, nil
		}),
	}
	do := func(channel, binding string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/callback/"+channel+"/"+binding, nil)
		req.SetPathValue("channel", channel)
		req.SetPathValue("binding", binding)
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, req)
		return rec
	}

	if rec := do("legacy", "b1"); rec.Code != http.StatusNotFound {
		t.Fatalf("binding dispatch to a non-BindingAware channel must 404, got %d", rec.Code)
	}

	// Unparseable config: dispatch still works, corp id is just empty.
	if rec := do("fake", "b1"); rec.Code != http.StatusOK || !ch.called {
		t.Fatalf("bad config jsonb must not fail the dispatch: status=%d called=%v",
			rec.Code, ch.called)
	}
	if ch.gotCreds.CorpID != "" {
		t.Fatalf("unparseable config must yield an empty corp id, got %q", ch.gotCreds.CorpID)
	}
}
