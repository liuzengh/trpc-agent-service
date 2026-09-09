package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	tagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// ModelSpec carries OpenAI-compatible model settings. tenant.model_config
// uses this shape directly; agent_app.config nests it under "model".
// Precedence: app config → tenant model_config → the service env default
// (AssemblerConfig.Defaults).
type ModelSpec struct {
	Name        string   `json:"name"`
	BaseURL     string   `json:"base_url"`
	APIKeyRef   string   `json:"api_key_ref"` // secret ref, never the key itself
	Temperature *float64 `json:"temperature"`
}

// mergeModel overlays non-zero fields of each later spec onto the former.
func mergeModel(base ModelSpec, overrides ...ModelSpec) ModelSpec {
	out := base
	for _, o := range overrides {
		if o.Name != "" {
			out.Name = o.Name
		}
		if o.BaseURL != "" {
			out.BaseURL = o.BaseURL
		}
		if o.APIKeyRef != "" {
			out.APIKeyRef = o.APIKeyRef
		}
		if o.Temperature != nil {
			out.Temperature = o.Temperature
		}
	}
	return out
}

// DefaultModelHosts is the model-endpoint allowlist a deployment gets when it
// configures none: the platform's own default endpoint host. Everything a user
// says travels to the endpoint, so the fallback is as closed as the platform
// default rather than "any host".
func DefaultModelHosts() []string { return []string{"api.deepseek.com"} }

// ValidateModelSpec rejects a model spec whose base_url falls outside the
// platform allowlist. Tenant/app config is platform data, not user data:
// a tenant that could pick an arbitrary endpoint would receive
// the full conversation content — including session history replayed into
// every run — at a host of its choosing.
//
// An empty base_url is valid: it inherits the platform default. The scheme
// requirement has no loopback exception — a tenant config may only ever
// point at an allowlisted https endpoint, full stop.
func ValidateModelSpec(spec ModelSpec, allowed []string) error {
	if spec.BaseURL == "" {
		return nil
	}
	u, err := url.Parse(spec.BaseURL)
	if err != nil || u.Hostname() == "" {
		return fmt.Errorf("model.base_url %q is not a valid URL", spec.BaseURL)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("model.base_url %q must use https", spec.BaseURL)
	}
	host := strings.ToLower(u.Hostname())
	if len(allowed) == 0 {
		allowed = DefaultModelHosts()
	}
	for _, h := range allowed {
		if host == strings.ToLower(strings.TrimSpace(h)) {
			return nil
		}
	}
	return fmt.Errorf("model.base_url host %q is not in the platform allowlist %v", host, allowed)
}

// ValidateModelConfig validates tenant.model_config JSON against the
// allowlist. Invalid JSON is an error here (unlike parseModelSpec, which
// degrades to the default with a warning) — a config that cannot be parsed
// must never reach the store.
func ValidateModelConfig(raw json.RawMessage, allowed []string) error {
	if len(raw) == 0 {
		return nil
	}
	var spec ModelSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return fmt.Errorf("parse model_config: %w", err)
	}
	return ValidateModelSpec(spec, allowed)
}

// ValidateAppConfig validates agent_app.config JSON (the model spec nests
// under "model") against the allowlist.
func ValidateAppConfig(raw json.RawMessage, allowed []string) error {
	if len(raw) == 0 {
		return nil
	}
	ac, err := parseAppConfig(raw)
	if err != nil {
		return err
	}
	return ValidateModelSpec(ac.Model, allowed)
}

// appConfig is the parsed agent_app.config JSONB. All fields are optional;
// unset fields inherit the tenant policy or the service default.
type appConfig struct {
	Prompt string          `json:"prompt"` // system instruction
	Model  ModelSpec       `json:"model"`
	Tools  tool.ToolPolicy `json:"tools"` // narrows the tenant tool whitelist
}

func parseAppConfig(raw json.RawMessage) (appConfig, error) {
	var c appConfig
	if len(raw) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("parse app config: %w", err)
	}
	return c, nil
}

func parseModelSpec(raw json.RawMessage) ModelSpec {
	var m ModelSpec
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			plog.Warnf("invalid tenant model_config ignored: %v", err)
		}
	}
	return m
}

func parseToolPolicy(raw json.RawMessage) tool.ToolPolicy {
	var p tool.ToolPolicy
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			plog.Warnf("invalid tenant tool_policy ignored: %v", err)
		}
	}
	return p
}

// StorageConfig is the tenant's storage backend override: empty means the
// platform default stack. Only session has a choice today (redis / postgres);
// unknown values fall back to the default with a warning. Changes arrive only
// through the migration flow — a direct edit is rejected by the Admin API.
type StorageConfig struct {
	Session struct {
		Type string `json:"type"` // "redis" | "postgres"
	} `json:"session"`
}

func parseStorageConfig(raw json.RawMessage) StorageConfig {
	var c StorageConfig
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			plog.Warnf("invalid tenant storage_config ignored: %v", err)
		}
	}
	return c
}

// AppProvider resolves the app a message was routed to, plus its owning
// tenant (*tenant.Resolver satisfies it).
type AppProvider interface {
	AppByID(ctx context.Context, appID string) (tenant.AgentApp, tenant.Tenant, error)
	// ActiveMigration reports a tenant's in-flight storage migration, nil when
	// none; while one is active the assembler dual-writes both backends.
	ActiveMigration(tenantID, resource string) *tenant.Migration
}

// keyError marks a model key resolution failure: the app is served by the
// echo processor for this message, but the failure is not cached so a
// repaired secret takes effect on the next message.
type keyError struct{ Err error }

func (e *keyError) Error() string { return e.Err.Error() }
func (e *keyError) Unwrap() error { return e.Err }

// Assembler builds a Runner-backed Processor per agent app and caches
// assemblies keyed by app ID. An entry is rebuilt whenever the app config or
// the tenant's model_config / tool_policy bytes change; the Resolver
// underneath refreshes on TTL plus the Admin API's pub/sub invalidation, so a
// publish/rollback propagates to workers within seconds.
//
// Replaced runners are not closed on eviction — an in-flight message may
// still hold one; they are closed all together at Close (process exit).
type Assembler struct {
	cfg AssemblerConfig

	mu    sync.RWMutex
	cache map[string]cacheEntry
	live  []*RunnerProcessor // every assembled runner, closed at Close
}

type cacheEntry struct {
	proc        Processor
	fingerprint []byte
}

// AssemblerConfig carries the shared dependencies every per-app assembly
// reuses: memory/knowledge backends are process-level, while model,
// prompt and tools come from the app config and tenant policies.
type AssemblerConfig struct {
	Apps      AppProvider // nil: env-only single-app mode (PG down at startup)
	Secrets   config.SecretResolver
	Registry  *tool.Registry
	Memory    memory.Service
	Knowledge knowledge.Knowledge
	Callbacks *ttool.Callbacks

	// SessionsByType holds one session service per supported backend
	// ("redis" / "postgres"); DefaultSession is the key of the
	// platform-recommended stack. The tenant's storage_config.session.type
	// routes its apps to the chosen backend; empty/unknown means the default.
	SessionsByType map[string]session.Service
	DefaultSession string
	// Artifact, when set, is wired onto every per-app runner.
	Artifact artifact.Service

	// Defaults is the env model config (lowest precedence); DefaultApp is the
	// runner app name for unrouted messages (TRPC_APP_NAME, routing disabled).
	// ModelHosts is the platform model-endpoint allowlist
	// (config.Config.ModelHostAllowlist): a tenant/app override pointing
	// elsewhere is refused at assembly time too — the Admin API gates the
	// write path, this gates everything that reaches the store another way.
	// Nil falls back to DefaultModelHosts.
	Defaults   ModelSpec
	ModelHosts []string
	DefaultApp string
	// Timeout / Retries for every per-app runner.
	Timeout time.Duration
	Retries int
}

// NewAssembler creates an Assembler.
func NewAssembler(cfg AssemblerConfig) *Assembler {
	return &Assembler{cfg: cfg, cache: make(map[string]cacheEntry)}
}

// Process implements Processor: dispatch the message to the runner assembled
// for msg.AppID (or to the default app when routing is disabled).
func (a *Assembler) Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	p, err := a.processorFor(ctx, msg.AppID)
	if err != nil {
		return channels.OutboundMessage{}, err
	}
	return p.Process(ctx, msg)
}

// processorFor returns the cached processor for the app, assembling it when
// the app config or tenant policies changed since the cached entry was built.
func (a *Assembler) processorFor(ctx context.Context, appID string) (Processor, error) {
	app, t, err := a.resolveApp(ctx, appID)
	if err != nil {
		return nil, err
	}
	fp := fingerprint(app.Config, t.ToolPolicy, t.ModelConfig, t.StorageConfig, migrationFingerprint(a.activeMigration(t.ID)))

	a.mu.RLock()
	entry, ok := a.cache[app.ID]
	a.mu.RUnlock()
	if ok && bytes.Equal(entry.fingerprint, fp) {
		return entry.proc, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if entry, ok := a.cache[app.ID]; ok && bytes.Equal(entry.fingerprint, fp) {
		return entry.proc, nil
	}
	proc, err := a.assemble(ctx, app, t)
	if err != nil {
		var ke *keyError
		if errors.As(err, &ke) {
			// Demoability over strictness: no model key means this app echoes.
			// Uncached on purpose — see keyError.
			plog.Warnf("model key for app %s unavailable (%v), serving echo", app.ID, ke.Err)
			return EchoProcessor{}, nil
		}
		return nil, fmt.Errorf("assemble app %s: %w", app.ID, err)
	}
	if rp, ok := proc.(*RunnerProcessor); ok {
		a.live = append(a.live, rp)
	}
	a.cache[app.ID] = cacheEntry{proc: proc, fingerprint: fp}
	plog.Infof("assembled runner for app %s (name=%s, version=%d)", app.ID, app.Name, app.Version)
	return proc, nil
}

// resolveApp maps the stamped app ID to its config and tenant. Messages with
// an empty app ID arrive only when gateway routing is disabled; they fall
// back to the env-configured default app (the single-tenant dev path).
func (a *Assembler) resolveApp(ctx context.Context, appID string) (tenant.AgentApp, tenant.Tenant, error) {
	if appID == "" {
		if a.cfg.Apps != nil && a.cfg.DefaultApp != "" {
			app, t, err := a.cfg.Apps.AppByID(ctx, a.cfg.DefaultApp)
			if err == nil {
				return app, t, nil
			}
			// Never take the tenant-less fallback silently: the app below runs
			// with no tenant policies at all, so the reason has to be in the
			// log next to the message it served.
			plog.Warnf("default app %s unresolved (%v), serving without tenant policies",
				a.cfg.DefaultApp, err)
		}
		return tenant.AgentApp{ID: a.cfg.DefaultApp, Name: "default"}, tenant.Tenant{}, nil
	}
	if a.cfg.Apps == nil {
		return tenant.AgentApp{}, tenant.Tenant{},
			fmt.Errorf("no app provider (PG down) for app %s", appID)
	}
	return a.cfg.Apps.AppByID(ctx, appID)
}

// assemble builds the RunnerProcessor for one app from its config, the tenant
// policies and the shared process-level dependencies.
func (a *Assembler) assemble(ctx context.Context, app tenant.AgentApp, t tenant.Tenant) (Processor, error) {
	ac, err := parseAppConfig(app.Config)
	if err != nil {
		return nil, err
	}
	spec := mergeModel(a.cfg.Defaults, parseModelSpec(t.ModelConfig), ac.Model)
	// Defense in depth: rows reach the store by other roads than the Admin
	// API too (direct SQL), so the allowlist is re-checked here. The platform
	// default endpoint skips the check; any tenant or app override must pass.
	if spec.BaseURL != a.cfg.Defaults.BaseURL {
		if err := ValidateModelSpec(spec, a.cfg.ModelHosts); err != nil {
			return nil, fmt.Errorf("app %s model endpoint rejected: %w", app.ID, err)
		}
	}
	key, err := a.cfg.Secrets.Resolve(ctx, spec.APIKeyRef)
	if err != nil {
		return nil, &keyError{Err: fmt.Errorf("resolve model key %q: %w", spec.APIKeyRef, err)}
	}

	// Tool isolation: the platform registry is narrowed by the tenant
	// whitelist first, then by the app's own policy.
	tools := a.cfg.Registry.Allowed(parseToolPolicy(t.ToolPolicy), ac.Tools)

	// Session backend routing: the tenant's storage_config picks from the
	// controlled menu; empty/unknown means the platform default. Session
	// history does not follow the runner across backends — switching happens
	// through the migration flow.
	sess := a.sessionServiceFor(t)

	// Knowledge isolation: the shared pgvector base is filtered down to this
	// tenant/app pair.
	filter := knowledgeFilter(t.ID, app.ID)
	return NewRunnerProcessor(RunnerConfig{
		AppName:         app.ID,
		BaseURL:         spec.BaseURL,
		APIKey:          key,
		ModelName:       spec.Name,
		Instruction:     ac.Prompt,
		Temperature:     spec.Temperature,
		SessionService:  sess,
		ArtifactService: a.cfg.Artifact,
		Timeout:         a.cfg.Timeout,
		Retries:         a.cfg.Retries,
		Tools:           tools,
		ToolCallbacks:   toolTimingCallbacks(a.cfg.Callbacks, t.ID),
		ModelCallbacks:  modelTimingCallbacks(t.ID, spec.Name),
		MemoryService:   a.cfg.Memory,
		Knowledge:       a.cfg.Knowledge,
		KnowledgeFilter: filter,
	}), nil
}

// sessionServiceFor picks the tenant's session backend from the controlled
// menu; empty or unknown types fall back to the platform default with a
// warning. During a migration the choice wraps in a dual-write fanout: reads
// follow the phase (old backend until the read switch, new one while
// observing), writes hit both.
func (a *Assembler) sessionServiceFor(t tenant.Tenant) session.Service {
	mig := a.activeMigration(t.ID)
	if mig != nil {
		primary, secondary := mig.FromBackend, mig.ToBackend
		if mig.Phase == tenant.PhaseObserving {
			primary, secondary = secondary, primary
		}
		p, pok := a.cfg.SessionsByType[primary]
		s, sok := a.cfg.SessionsByType[secondary]
		if pok && sok {
			return &storage.FanoutSessionService{Primary: p, Secondary: s}
		}
		plog.Warnf("tenant %s migration backend pair %s/%s unavailable, using default %s",
			t.ID, primary, secondary, a.cfg.DefaultSession)
	}
	def := a.cfg.SessionsByType[a.cfg.DefaultSession]
	st := parseStorageConfig(t.StorageConfig).Session.Type
	if st == "" {
		return def
	}
	alt, ok := a.cfg.SessionsByType[st]
	if !ok {
		plog.Warnf("tenant %s storage_config session type %q unavailable, using default %s",
			t.ID, st, a.cfg.DefaultSession)
		return def
	}
	return alt
}

// activeMigration returns the tenant's in-flight session migration, nil when
// routing is disabled or none is active.
func (a *Assembler) activeMigration(tenantID string) *tenant.Migration {
	if a.cfg.Apps == nil || tenantID == "" {
		return nil
	}
	return a.cfg.Apps.ActiveMigration(tenantID, "session")
}

// SessionServiceFor resolves the session backend of the given app (tenant
// routing + migration fanout), for out-of-band state writes such as the
// recall marker.
func (a *Assembler) SessionServiceFor(ctx context.Context, appID string) (session.Service, error) {
	if a.cfg.Apps == nil {
		return nil, errors.New("no app provider")
	}
	_, t, err := a.cfg.Apps.AppByID(ctx, appID)
	if err != nil {
		return nil, err
	}
	return a.sessionServiceFor(t), nil
}

// migrationFingerprint distinguishes assemblies built under different
// migration states so a phase change rebuilds the runner.
func migrationFingerprint(m *tenant.Migration) []byte {
	if m == nil {
		return nil
	}
	return []byte(m.FromBackend + ">" + m.ToBackend + "@" + m.Phase)
}

// Close shuts down every runner the assembler created (process exit).
func (a *Assembler) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var first error
	for _, p := range a.live {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
	}
	a.live = nil
	a.cache = make(map[string]cacheEntry)
	return first
}

// fingerprint identifies the inputs an assembly depends on; a change in any
// of them (publish, rollback, policy edit) rebuilds the runner. JSONB columns
// round-trip in normalized form, so byte equality matches logical equality.
func fingerprint(parts ...json.RawMessage) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, 0)
		out = append(out, p...)
	}
	return out
}

// timingStaleTTL bounds how long an unmatched callback start timestamp is
// kept: a Before whose After never fires (the model call failed before any
// response was produced, so the framework skips the After callback) would
// otherwise accumulate one map entry per failed call forever. Ten minutes is
// far past any per-run deadline (RunnerConfig.Timeout).
const timingStaleTTL = 10 * time.Minute

// callTimer hands a start timestamp from a Before callback to its After
// counterpart. Starts are keyed per in-flight call and deleted on After, so
// the map holds only in-flight calls plus the rare unmatched entries the
// opportunistic sweep in begin has not reaped yet.
type callTimer struct {
	starts sync.Map // call key -> time.Time
}

func (t *callTimer) begin(key string) {
	now := time.Now()
	t.starts.Range(func(k, v any) bool {
		if ts, ok := v.(time.Time); ok && now.Sub(ts) > timingStaleTTL {
			t.starts.Delete(k)
		}
		return true
	})
	t.starts.Store(key, now)
}

// end reports the elapsed time for a begun call, false when the key is
// unknown. Unknown means After without a matching Before — e.g. a second
// After for a response stream that yielded several responses records only
// once, and an approval-blocked tool call never began.
func (t *callTimer) end(key string) (time.Duration, bool) {
	v, ok := t.starts.LoadAndDelete(key)
	if !ok {
		return 0, false
	}
	ts, ok := v.(time.Time)
	if !ok {
		return 0, false
	}
	return time.Since(ts), true
}

// pending counts the in-flight (or not-yet-reaped) starts; tests use it to
// assert the map drains back to zero.
func (t *callTimer) pending() int {
	n := 0
	t.starts.Range(func(_, _ any) bool { n++; return true })
	return n
}

// modelTimer measures per-call model latency (metrics.LLMCallDuration) for
// one assembled runner; tenant and model are baked in at assembly time
// because the callback context does not carry the tenant ID. Calls are keyed
// by invocation ID: one invocation's tool loop runs its model calls
// sequentially, so the key is never shared by overlapping calls. A Before
// whose After never fires (the call failed before any response was produced)
// leaves no duration point; its start entry is reaped by the sweep.
type modelTimer struct {
	tenantID string
	model    string
	starts   callTimer
}

func (t *modelTimer) before(ctx context.Context, _ *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
	inv, ok := tagent.InvocationFromContext(ctx)
	if !ok || inv == nil {
		return nil, nil
	}
	t.starts.begin(inv.InvocationID)
	return nil, nil
}

func (t *modelTimer) after(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
	inv, ok := tagent.InvocationFromContext(ctx)
	if !ok || inv == nil {
		return nil, nil
	}
	elapsed, ok := t.starts.end(inv.InvocationID)
	if !ok {
		return nil, nil
	}
	metrics.LLMCallDuration.Record(ctx,
		float64(elapsed)/float64(time.Millisecond),
		callDurationAttrs(t.tenantID, "model", t.model, args.Error))
	return nil, nil
}

// modelTimingCallbacks registers a modelTimer as the runner's model callbacks.
func modelTimingCallbacks(tenantID, modelName string) *model.Callbacks {
	t := &modelTimer{tenantID: tenantID, model: modelName}
	return model.NewCallbacks().
		RegisterBeforeModel(model.BeforeModelCallbackStructured(t.before)).
		RegisterAfterModel(model.AfterModelCallbackStructured(t.after))
}

// toolTimer measures tool execution latency (metrics.ToolCallDuration) for
// one assembled runner. Calls are keyed by ToolCallID, which the framework
// guarantees to be unique per tool call issued by the model; an empty ID is
// not timed — such calls would share one map key and overwrite each other's
// start time.
type toolTimer struct {
	tenantID string
	starts   callTimer
}

func (t *toolTimer) before(_ context.Context, args *ttool.BeforeToolArgs) (*ttool.BeforeToolResult, error) {
	if args.ToolCallID == "" {
		return nil, nil
	}
	t.starts.begin(args.ToolCallID)
	return nil, nil
}

func (t *toolTimer) after(ctx context.Context, args *ttool.AfterToolArgs) (*ttool.AfterToolResult, error) {
	elapsed, ok := t.starts.end(args.ToolCallID)
	if !ok {
		return nil, nil
	}
	metrics.ToolCallDuration.Record(ctx,
		float64(elapsed)/float64(time.Millisecond),
		callDurationAttrs(t.tenantID, "tool", args.ToolName, args.Error))
	return nil, nil
}

// toolTimingCallbacks appends a toolTimer to the shared tool callbacks (which
// carry the approval Approver's BeforeTool), cloned so the shared instance is
// never mutated. The timing Before is appended AFTER the approver and always
// returns (nil, nil): a blocked call's CustomResult short-circuits the Before
// chain before the timer starts, so pass-through and interception semantics
// are unchanged — and since the framework skips After on a Before
// short-circuit, a blocked call has no duration point (by design: it never
// executed).
func toolTimingCallbacks(base *ttool.Callbacks, tenantID string) *ttool.Callbacks {
	cbs := base.Clone()
	if cbs == nil {
		cbs = ttool.NewCallbacks()
	}
	t := &toolTimer{tenantID: tenantID}
	return cbs.
		RegisterBeforeTool(ttool.BeforeToolCallbackStructured(t.before)).
		RegisterAfterTool(ttool.AfterToolCallbackStructured(t.after))
}

// callDurationAttrs tags a call-latency sample by tenant, the named subject
// (model name or tool name under the given label key) and the result.
func callDurationAttrs(tenantID, subjectKey, subject string, callErr error) otelmetric.MeasurementOption {
	result := "ok"
	if callErr != nil {
		result = "error"
	}
	return otelmetric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String(subjectKey, subject),
		attribute.String("result", result),
	)
}
