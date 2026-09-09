package web

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// BindingLookup answers binding-id → channel_binding row. *tenant.Resolver
// satisfies it; the interface keeps the dispatcher testable without PG.
type BindingLookup interface {
	BindingByID(ctx context.Context, id string) (tenant.ChannelBinding, error)
}

// BindingDispatcher serves the multi-tenant callback paths
// /callback/{channel}/{binding_id}: one stable route
// pattern, many bindings. The dispatcher resolves the binding row, hands the
// request to the owning adapter together with the binding's credential
// references, and the adapter verifies the callback with those keys before any
// payload is parsed. A binding created (or activated) after startup becomes
// reachable on its next snapshot refresh — TTL plus the admin invalidation
// broadcast — without touching the mux.
//
// Authorization stays on the shared pipeline: the adapter's normalized
// message carries the callback path into EnqueueHandler, which re-resolves
// the route and enforces binding/tenant/app status, rate limits and
// guardrails. This layer only routes bytes to the right verifier.
type BindingDispatcher struct {
	// Channels is the process channel registry (mock/wecom/wxkf).
	Channels map[string]channels.Channel
	// Bindings resolves binding ids; nil fails closed.
	Bindings BindingLookup
	// Handler is the shared inbound pipeline (EnqueueHandler).
	Handler channels.Handler
}

func (d BindingDispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	channelName := r.PathValue("channel")
	bindingID := r.PathValue("binding")

	ch, ok := d.Channels[channelName]
	if !ok {
		http.Error(w, "unknown channel", http.StatusNotFound)
		return
	}
	ba, ok := ch.(channels.BindingAware)
	if !ok {
		// A channel without per-binding support keeps serving its
		// env-configured callback path only.
		http.Error(w, "channel does not support bindings", http.StatusNotFound)
		return
	}
	if d.Bindings == nil {
		http.Error(w, "binding lookup unavailable", http.StatusServiceUnavailable)
		return
	}
	b, err := d.Bindings.BindingByID(r.Context(), bindingID)
	if err != nil {
		plog.Warnf("binding dispatch %s/%s: %v", channelName, bindingID, err)
		http.Error(w, "unknown binding", http.StatusNotFound)
		return
	}
	if b.Channel != channelName {
		http.Error(w, "binding channel mismatch", http.StatusNotFound)
		return
	}

	handler, err := ba.CallbackHandler(d.Handler, channels.BindingCredentials{
		BindingID: b.ID,
		CorpID:    bindingCorpID(b),
		TokenRef:  b.TokenRef,
		AESKeyRef: b.AESKeyRef,
	})
	if err != nil {
		// Credential references unresolvable: 5xx so the IM redelivers once
		// the secret store recovers.
		plog.Errorf("binding %s callback handler: %v", b.ID, err)
		http.Error(w, "binding credentials unavailable", http.StatusServiceUnavailable)
		return
	}
	handler(w, r)
}

// bindingCorpID reads an optional {"corp_id": "..."} override from the
// binding's config jsonb: two tenants on one channel may belong to different
// IM corps, and the crypt receiver id must match the credential pair.
func bindingCorpID(b tenant.ChannelBinding) string {
	if len(b.Config) == 0 {
		return ""
	}
	var cfg struct {
		CorpID string `json:"corp_id"`
	}
	if err := json.Unmarshal(b.Config, &cfg); err != nil {
		plog.Warnf("binding %s config parse: %v", b.ID, err)
		return ""
	}
	return cfg.CorpID
}
