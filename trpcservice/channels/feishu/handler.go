package feishu

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const callbackPathPrefix = "/channels/feishu/"

// BindingProvider resolves the enabled callback bindings for one binding_id
// from the control plane at request time. Multiple tenants may share a
// binding_id, so it returns every candidate; the handler disambiguates with
// the server-owned Verification Token.
type BindingProvider func(bindingID string) []Binding

// Handler verifies Feishu event-subscription callbacks and hands normalized
// messages to the shared durable ingress pipeline. It acknowledges quickly
// and never waits for Agent execution.
type Handler struct {
	provider BindingProvider
	acceptor gateway.InboundAcceptor
	now      func() time.Time
	media    func(Binding) channels.MediaDownloader
	policy   channels.MediaPolicy
}

// NewDynamicHandlerWithMedia enables controlled media downloads after event
// authentication and before the durable Inbox write.
func NewDynamicHandlerWithMedia(acceptor gateway.InboundAcceptor, provider BindingProvider, media func(Binding) channels.MediaDownloader, policy channels.MediaPolicy) (*Handler, error) {
	handler, err := NewDynamicHandler(acceptor, provider)
	if err != nil {
		return nil, err
	}
	if media == nil {
		return nil, errors.New("feishu: media downloader provider is required")
	}
	handler.media, handler.policy = media, policy
	return handler, nil
}

// NewDynamicHandler resolves bindings per callback from the control plane.
func NewDynamicHandler(acceptor gateway.InboundAcceptor, provider BindingProvider) (*Handler, error) {
	if acceptor == nil || provider == nil {
		return nil, errors.New("feishu: inbound acceptor and binding provider are required")
	}
	return &Handler{provider: provider, acceptor: acceptor, now: time.Now}, nil
}

// ServeHTTP implements the Feishu callback endpoint. Feishu only uses POST:
// URL verification and events share the same path.
func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	candidates, ok := handler.bindings(request.URL.Path)
	if !ok {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 1<<20))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid callback body")
		return
	}
	var raw struct {
		Encrypt string `json:"encrypt"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid callback body")
		return
	}
	payload := body
	encrypted := strings.TrimSpace(raw.Encrypt) != ""
	if encrypted {
		payload, candidates, ok = handler.decryptCandidates(candidates, raw.Encrypt)
		if !ok {
			writeError(writer, http.StatusUnauthorized, "invalid encrypted callback")
			return
		}
	} else {
		candidates = plaintextCandidates(candidates)
		if len(candidates) == 0 {
			// Encryption is enabled for every candidate binding: never fall
			// back to plaintext silently.
			writeError(writer, http.StatusUnauthorized, "plaintext callback is not accepted")
			return
		}
	}
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid callback body")
		return
	}
	if probe.Type == "url_verification" {
		// Feishu's official dispatcher decrypts URL verification first and
		// authenticates it with the Verification Token, but does not require
		// the normal event signature. Requiring X-Lark-Signature here rejects
		// the platform's initial encrypted challenge.
		handler.verifyURL(writer, candidates, payload)
		return
	}
	if encrypted {
		candidates = signedCandidates(request, candidates, body)
		if len(candidates) == 0 {
			writeError(writer, http.StatusUnauthorized, "invalid callback signature")
			return
		}
	}
	handler.receive(writer, request, candidates, payload)
}

// bindings extracts the binding_id from the URL path. It only selects
// server-side control-plane configuration; tenant and app scope can never be
// supplied by the client.
func (handler *Handler) bindings(path string) ([]Binding, bool) {
	if handler == nil || handler.provider == nil || !strings.HasPrefix(path, callbackPathPrefix) {
		return nil, false
	}
	raw := strings.TrimPrefix(path, callbackPathPrefix)
	if raw == "" || strings.Contains(raw, "/") {
		return nil, false
	}
	bindingID, err := url.PathUnescape(raw)
	if err != nil || bindingID == "" {
		return nil, false
	}
	candidates := handler.provider(bindingID)
	if len(candidates) == 0 {
		return nil, false
	}
	return candidates, true
}

// decryptCandidates tries the configured candidates and returns the
// plaintext plus only the candidates that decrypted to that exact payload.
// Keeping the narrowed set prevents a token belonging to another tenant from
// being selected after one candidate's Encrypt Key authenticated the body.
func (handler *Handler) decryptCandidates(candidates []Binding, encoded string) ([]byte, []Binding, bool) {
	var payload []byte
	var matched []Binding
	for _, binding := range candidates {
		if binding.EncryptKey == "" {
			continue
		}
		plain, err := decrypt(binding.EncryptKey, encoded)
		if err != nil {
			continue
		}
		if payload == nil {
			payload = plain
		}
		if subtle.ConstantTimeCompare(payload, plain) == 1 {
			matched = append(matched, binding)
		}
	}
	return payload, matched, payload != nil && len(matched) > 0
}

// signedCandidates verifies the official Feishu event signature over the raw
// request body. A configured Encrypt Key never accepts a missing signature.
// The result is also a tenant-candidate filter because the key is server-owned.
func signedCandidates(request *http.Request, candidates []Binding, body []byte) []Binding {
	timestamp := strings.TrimSpace(request.Header.Get("X-Lark-Request-Timestamp"))
	nonce := strings.TrimSpace(request.Header.Get("X-Lark-Request-Nonce"))
	signature := strings.TrimSpace(request.Header.Get("X-Lark-Signature"))
	if timestamp == "" || nonce == "" || signature == "" {
		return nil
	}
	var matched []Binding
	for _, binding := range candidates {
		if binding.EncryptKey == "" {
			continue
		}
		digest := sha256.Sum256([]byte(timestamp + nonce + binding.EncryptKey + string(body)))
		expected := hex.EncodeToString(digest[:])
		if len(expected) == len(signature) && subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) == 1 {
			matched = append(matched, binding)
		}
	}
	return matched
}

// plaintextCandidates keeps only bindings without an Encrypt Key.
func plaintextCandidates(candidates []Binding) []Binding {
	var plain []Binding
	for _, binding := range candidates {
		if binding.EncryptKey == "" {
			plain = append(plain, binding)
		}
	}
	return plain
}

// verifyURL answers the URL verification handshake with the challenge when
// one candidate's Verification Token matches.
func (handler *Handler) verifyURL(writer http.ResponseWriter, candidates []Binding, payload []byte) {
	var verification urlVerification
	if err := json.Unmarshal(payload, &verification); err != nil || strings.TrimSpace(verification.Challenge) == "" {
		writeError(writer, http.StatusBadRequest, "invalid verification body")
		return
	}
	if !hasTokenCandidate(candidates, verification.Token) {
		writeError(writer, http.StatusUnauthorized, "invalid verification token")
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(map[string]string{"challenge": verification.Challenge})
}

// receive validates one v2 event and hands it to the durable ingress.
func (handler *Handler) receive(writer http.ResponseWriter, request *http.Request, candidates []Binding, payload []byte) {
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid callback body")
		return
	}
	binding := matchEventCandidate(candidates, env.Header.Token, env.Header.AppID)
	if binding == nil {
		writeError(writer, http.StatusUnauthorized, "invalid verification token")
		return
	}
	inbound, err := normalize(*binding, env, handler.now())
	if err != nil {
		switch {
		case errors.Is(err, ErrUnsupportedMessage):
			// Unsupported provider events are poison messages for the Agent
			// path. Ack them so Feishu does not retry.
			writeAccepted(writer)
		case errors.Is(err, ErrBindingMismatch):
			writeError(writer, http.StatusUnauthorized, "callback binding mismatch")
		default:
			writeError(writer, http.StatusBadRequest, "invalid callback message")
		}
		return
	}
	if inbound.Media != nil {
		ref := *inbound.Media
		inbound.Media = nil
		if handler.media != nil {
			attachment, mediaErr := channels.LoadMedia(request.Context(), handler.media(*binding), ref, handler.policy)
			if mediaErr != nil {
				writeError(writer, http.StatusServiceUnavailable, "temporarily unable to process media")
				return
			}
			inbound.Attachments = []gateway.Attachment{attachment}
		}
	}
	if _, err := handler.acceptor.AcceptInbound(request.Context(), inbound); err != nil {
		// A 5xx response asks Feishu to retry a temporary Inbox/queue failure.
		writeError(writer, http.StatusServiceUnavailable, "temporarily unable to accept callback")
		return
	}
	writeAccepted(writer)
}

// hasTokenCandidate authenticates a URL-verification challenge. Challenges do
// not route tenant data, so one or more matching candidates are equally safe.
func hasTokenCandidate(candidates []Binding, token string) bool {
	if token == "" {
		return false
	}
	for _, candidate := range candidates {
		expected := candidate.VerificationToken
		if expected != "" && len(expected) == len(token) && subtle.ConstantTimeCompare([]byte(expected), []byte(token)) == 1 {
			return true
		}
	}
	return false
}

// matchEventCandidate requires both the secret token and event app_id and
// rejects ambiguity instead of routing to whichever database row came first.
func matchEventCandidate(candidates []Binding, token, appID string) *Binding {
	if token == "" || appID == "" {
		return nil
	}
	var matched *Binding
	for i := range candidates {
		expected := candidates[i].VerificationToken
		if candidates[i].FeishuAppID != appID || expected == "" || len(expected) != len(token) || subtle.ConstantTimeCompare([]byte(expected), []byte(token)) != 1 {
			continue
		}
		if matched != nil {
			return nil
		}
		matched = &candidates[i]
	}
	return matched
}

func writeAccepted(writer http.ResponseWriter) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(`{"code":0}`))
}

// writeError never includes secrets, encrypted payloads, or user content.
func writeError(writer http.ResponseWriter, status int, message string) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": message})
}
