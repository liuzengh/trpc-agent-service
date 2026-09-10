package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	a2aprotocol "trpc.group/trpc-go/trpc-a2a-go/protocol"
	a2aserverapi "trpc.group/trpc-go/trpc-a2a-go/server"
	a2ataskmanager "trpc.group/trpc-go/trpc-a2a-go/taskmanager"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	a2aserver "trpc.group/trpc-go/trpc-agent-go/server/a2a"
	openaiserver "trpc.group/trpc-go/trpc-agent-go/server/openai"
	trpcagentserver "trpc.group/trpc-go/trpc-agent-go/server/trpcagent"
)

// ProtocolHTTPOptions describes protocol façades that are safe to expose from
// the Gateway role. Runner must be the durable bridge adapter, never a local
// agent runner.
type ProtocolHTTPOptions struct {
	Runner     runner.Runner
	Resolver   ProtocolInvocationResolver
	Readiness  interface{ Ready() bool }
	MaxBody    int64
	RunTimeout time.Duration
	A2A        *DurableA2ATaskManager
	PublicURL  string
}

// NewProtocolHTTPHandler mounts OpenAI, tRPC-Agent, and durable A2A over one
// authenticated, readiness-gated boundary.
func NewProtocolHTTPHandler(options ProtocolHTTPOptions) (http.Handler, error) {
	if options.Runner == nil || options.Resolver == nil || options.Readiness == nil || options.MaxBody < 1 || options.RunTimeout <= 0 ||
		options.A2A == nil || !options.A2A.valid() || !validGatewayPublicURL(options.PublicURL) {
		return nil, runtime.ErrInvariantViolation
	}
	openai, err := openaiserver.New(openaiserver.WithRunner(options.Runner))
	if err != nil {
		return nil, err
	}
	trpcFacade := TRPCAgentFacade{Runner: options.Runner, Timeout: options.RunTimeout}
	a2aFacade := A2AFacade{Runner: options.Runner, Tasks: options.A2A, PublicURL: strings.TrimSuffix(options.PublicURL, "/")}
	routes := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/chat/completions":
			openai.Handler().ServeHTTP(w, r)
		case protocolForPath(r.URL.Path) == "trpc-agent":
			trpcFacade.ServeHTTP(w, r)
		case protocolForPath(r.URL.Path) == "a2a":
			a2aFacade.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	guarded := protocolAdmission{Readiness: options.Readiness, MaxBody: options.MaxBody, RunTimeout: options.RunTimeout, Next: routes}
	return ProtocolInvocationMiddleware{Resolver: options.Resolver, Next: guarded}, nil
}

// A2AFacade exposes the upstream JSON-RPC wire format over the durable task
// manager. Agent cards are tenant/app scoped and advertise no unsupported push
// notification or process-local history capability.
type A2AFacade struct {
	Runner    runner.Runner
	Tasks     *DurableA2ATaskManager
	PublicURL string
}

func (h A2AFacade) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	trusted, ok := ServerInvocationFromContext(r.Context())
	appName, validPath := a2aAppForPath(r.URL.Path)
	if h.Runner == nil || h.Tasks == nil || !h.Tasks.valid() || !ok || trusted.Protocol != "a2a" || !validPath || appName != trusted.Tenant.AgentAppID || !validGatewayPublicURL(h.PublicURL) {
		writeControlError(w, ErrUnauthenticated)
		return
	}
	if err := authorizeA2ARequest(r, trusted, h.Tasks.Readiness); err != nil {
		writeControlError(w, err)
		return
	}
	streaming, push, history := true, false, false
	scheme, bearerFormat := "bearer", "gw1"
	basePath := "/a2a/v1/apps/" + appName
	card := a2aserverapi.AgentCard{Name: appName, Description: "Durable trpc-agent-service A2A gateway", URL: h.PublicURL + basePath + "/",
		Version: "gateway-durable-v1", Capabilities: a2aserverapi.AgentCapabilities{Streaming: &streaming, PushNotifications: &push, StateTransitionHistory: &history},
		SecuritySchemes: map[string]a2aserverapi.SecurityScheme{"gatewayBearer": {Type: a2aserverapi.SecuritySchemeTypeHTTP, Scheme: &scheme, BearerFormat: &bearerFormat}},
		Security:        []map[string][]string{{"gatewayBearer": {}}}, DefaultInputModes: []string{"text"}, DefaultOutputModes: []string{"text"},
		Skills: []a2aserverapi.AgentSkill{{ID: appName, Name: appName, Tags: []string{"agent"}, InputModes: []string{"text"}, OutputModes: []string{"text"}}}}
	// The public trpc-agent-go A2A façade owns protocol decoding and event
	// conversion. Its task-manager hook deliberately returns the service's
	// durable manager: no process-local A2A task map is constructed or used.
	// The supplied runner is the GatewayRunnerBridge, not a local Agent runner.
	server, err := a2aserver.New(
		a2aserver.WithRunner(h.Runner),
		a2aserver.WithAgentCard(card),
		a2aserver.WithTaskManagerBuilder(func(a2ataskmanager.MessageProcessor) a2ataskmanager.TaskManager {
			return h.Tasks
		}),
		a2aserver.WithADKCompatibility(false),
	)
	if err != nil {
		writeControlError(w, runtime.ErrCapabilityUnsupported)
		return
	}
	server.Handler().ServeHTTP(w, r)
}

func authorizeA2ARequest(r *http.Request, trusted ServerInvocationContext, readiness interface{ Ready() bool }) error {
	if r.Method == http.MethodGet {
		if !trusted.CanRead {
			return ErrForbidden
		}
		return nil
	}
	if r.Method != http.MethodPost {
		return nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return runtime.ErrInvalidEnvelope
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var envelope struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Method == "" {
		return runtime.ErrInvalidEnvelope
	}
	switch envelope.Method {
	case a2aprotocol.MethodMessageSend, a2aprotocol.MethodMessageStream:
		if !trusted.CanRun {
			return ErrForbidden
		}
		if readiness == nil || !readiness.Ready() {
			return runtime.ErrBackendUnavailable
		}
	case a2aprotocol.MethodTasksGet, a2aprotocol.MethodTasksResubscribe, a2aprotocol.MethodAgentAuthenticatedExtendedCard:
		if !trusted.CanRead {
			return ErrForbidden
		}
	case a2aprotocol.MethodTasksCancel:
		if !trusted.CanCancel {
			return ErrForbidden
		}
	}
	return nil
}

func validGatewayPublicURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil &&
		(parsed.Path == "" || parsed.Path == "/") && parsed.RawQuery == "" && parsed.Fragment == ""
}

// TRPCAgentFacade resolves the app from the already authenticated path and
// constructs the stateless upstream façade. It owns no session or task state.
type TRPCAgentFacade struct {
	Runner  runner.Runner
	Timeout time.Duration
}

func (h TRPCAgentFacade) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	trusted, ok := ServerInvocationFromContext(r.Context())
	appName, validPath := trpcAgentRunApp(r.URL.Path)
	if h.Runner == nil || h.Timeout <= 0 || !ok || trusted.Protocol != "trpc-agent" || !validPath || appName != trusted.Tenant.AgentAppID {
		writeControlError(w, ErrUnauthenticated)
		return
	}
	server, err := trpcagentserver.New(trpcagentserver.WithAppName(appName), trpcagentserver.WithRunner(h.Runner), trpcagentserver.WithTimeout(h.Timeout))
	if err != nil {
		writeControlError(w, runtime.ErrCapabilityUnsupported)
		return
	}
	server.Handler().ServeHTTP(w, r)
}

type protocolAdmission struct {
	Readiness  interface{ Ready() bool }
	MaxBody    int64
	RunTimeout time.Duration
	Next       http.Handler
}

func (m protocolAdmission) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.Readiness == nil || m.Next == nil || m.MaxBody < 1 || m.RunTimeout <= 0 {
		writeControlError(w, runtime.ErrCapabilityUnsupported)
		return
	}
	if r.Method == http.MethodPost && protocolForPath(r.URL.Path) != "a2a" && !m.Readiness.Ready() {
		writeControlError(w, runtime.ErrBackendUnavailable)
		return
	}
	if r.ContentLength > m.MaxBody {
		http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, m.MaxBody)
	ctx, cancel := context.WithTimeout(r.Context(), m.RunTimeout)
	defer cancel()
	m.Next.ServeHTTP(w, r.WithContext(ctx))
}
