package wecom

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const callbackPathPrefix = "/channels/wecom/"

// BindingProvider resolves every enabled callback candidate at request time.
// Binding IDs are tenant-scoped, so the callback signature and receive ID
// must uniquely select one candidate before routing.
type BindingProvider func(bindingID string) []Binding

// Handler verifies WeCom callbacks and hands normalized messages to the shared
// durable ingress pipeline. It never waits for Agent execution.
type Handler struct {
	bindings map[string]Binding
	provider BindingProvider
	acceptor gateway.InboundAcceptor
	now      func() time.Time
	media    func(Binding) channels.MediaDownloader
	policy   channels.MediaPolicy
}

// NewHandler creates an immutable tenant binding registry.
func NewHandler(acceptor gateway.InboundAcceptor, bindings ...Binding) (*Handler, error) {
	if acceptor == nil {
		return nil, errors.New("wecom: inbound acceptor is required")
	}
	handler := &Handler{bindings: make(map[string]Binding, len(bindings)), acceptor: acceptor, now: time.Now}
	for _, binding := range bindings {
		if err := binding.validate(); err != nil {
			return nil, err
		}
		if _, exists := handler.bindings[binding.BindingID]; exists {
			return nil, fmt.Errorf("wecom: duplicate binding %q", binding.BindingID)
		}
		handler.bindings[binding.BindingID] = binding
	}
	if len(handler.bindings) == 0 {
		return nil, errors.New("wecom: at least one binding is required")
	}
	return handler, nil
}

// NewDynamicHandler resolves bindings per callback from the control plane.
func NewDynamicHandler(acceptor gateway.InboundAcceptor, provider BindingProvider) (*Handler, error) {
	if acceptor == nil || provider == nil {
		return nil, errors.New("wecom: inbound acceptor and binding provider are required")
	}
	return &Handler{provider: provider, acceptor: acceptor, now: time.Now}, nil
}

// NewDynamicHandlerWithMedia enables controlled media downloads after
// callback authentication and before the durable Inbox write.
func NewDynamicHandlerWithMedia(acceptor gateway.InboundAcceptor, provider BindingProvider, media func(Binding) channels.MediaDownloader, policy channels.MediaPolicy) (*Handler, error) {
	handler, err := NewDynamicHandler(acceptor, provider)
	if err != nil {
		return nil, err
	}
	if media == nil {
		return nil, errors.New("wecom: media downloader provider is required")
	}
	handler.media, handler.policy = media, policy
	return handler, nil
}

// ServeHTTP implements GET callback verification and POST message receipt.
func (handler *Handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	bindings := handler.bindingsFor(request.URL.Path)
	if len(bindings) == 0 {
		http.NotFound(w, request)
		return
	}
	switch request.Method {
	case http.MethodGet:
		handler.verifyURL(w, request, bindings)
	case http.MethodPost:
		handler.receive(w, request, bindings)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (handler *Handler) bindingsFor(path string) []Binding {
	if handler == nil || !strings.HasPrefix(path, callbackPathPrefix) {
		return nil
	}
	raw := strings.TrimPrefix(path, callbackPathPrefix)
	if raw == "" || strings.Contains(raw, "/") {
		return nil
	}
	bindingID, err := url.PathUnescape(raw)
	if err != nil {
		return nil
	}
	if handler.provider != nil {
		return handler.provider(bindingID)
	}
	binding, ok := handler.bindings[bindingID]
	if !ok {
		return nil
	}
	return []Binding{binding}
}

func (handler *Handler) verifyURL(w http.ResponseWriter, request *http.Request, bindings []Binding) {
	query := request.URL.Query()
	var matched [][]byte
	for _, binding := range bindings {
		plain, err := binding.Crypt.VerifyAndDecrypt(query.Get("msg_signature"), query.Get("timestamp"), query.Get("nonce"), query.Get("echostr"))
		if err == nil {
			matched = append(matched, plain)
		}
	}
	if len(matched) != 1 {
		http.Error(w, "invalid callback verification", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(matched[0])
}

func (handler *Handler) receive(w http.ResponseWriter, request *http.Request, bindings []Binding) {
	body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, 1<<20))
	if err != nil {
		http.Error(w, "read callback", http.StatusBadRequest)
		return
	}
	envelope, err := decodeEnvelope(body)
	if err != nil {
		http.Error(w, "invalid callback body", http.StatusBadRequest)
		return
	}
	query := request.URL.Query()
	type match struct {
		binding Binding
		inbound gateway.InboundMessage
	}
	var matched []match
	authenticatedUnsupported := 0
	for _, binding := range bindings {
		plain, decryptErr := binding.Crypt.VerifyAndDecrypt(query.Get("msg_signature"), query.Get("timestamp"), query.Get("nonce"), envelope.Encrypt)
		if decryptErr != nil {
			continue
		}
		message, decodeErr := decodeCallback(plain)
		if decodeErr != nil {
			http.Error(w, "invalid callback message", http.StatusBadRequest)
			return
		}
		inbound, normalizeErr := normalize(binding, message, handler.now())
		if errors.Is(normalizeErr, ErrBindingMismatch) {
			continue
		}
		if errors.Is(normalizeErr, ErrUnsupportedMessage) {
			authenticatedUnsupported++
			continue
		}
		if normalizeErr != nil {
			http.Error(w, "invalid callback message", http.StatusBadRequest)
			return
		}
		matched = append(matched, match{binding: binding, inbound: inbound})
	}
	if len(matched) == 0 && authenticatedUnsupported == 1 {
		writeSuccess(w)
		return
	}
	if len(matched) != 1 {
		http.Error(w, "invalid callback signature", http.StatusUnauthorized)
		return
	}
	inbound := matched[0].inbound
	if inbound.Media != nil {
		ref := *inbound.Media
		inbound.Media = nil
		if handler.media != nil {
			attachment, err := channels.LoadMedia(request.Context(), handler.media(matched[0].binding), ref, handler.policy)
			if err != nil {
				http.Error(w, "temporarily unable to process media", http.StatusServiceUnavailable)
				return
			}
			inbound.Attachments = []gateway.Attachment{attachment}
		}
	}
	if _, err := handler.acceptor.AcceptInbound(request.Context(), inbound); err != nil {
		// A non-200 response asks WeCom to retry a temporary Inbox/queue failure.
		http.Error(w, "temporarily unable to accept callback", http.StatusServiceUnavailable)
		return
	}
	writeSuccess(w)
}

func writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}
