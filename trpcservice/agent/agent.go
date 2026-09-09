// Package agent builds and owns tRPC-Agent-Go agent runtimes.
package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	openaiserver "trpc.group/trpc-go/trpc-agent-go/server/openai"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	DemoAppName   = "t/demo/a/echo"
	DemoAgentName = "echo-assistant"
	DemoModelName = "deterministic-echo"
)

// maxToolIterations bounds how many rounds of tool calls one run may take. An
// iteration is one assistant response that contains tool calls, so the bound is
// on model-tool-model loops, not on the number of tools called.
//
// It exists because a run holds a session lease for as long as it lasts. A
// model that keeps re-issuing tool calls — a loop it cannot see, a tool error
// it retries forever — would hold that lease and the upstream connection for as
// long as the model kept talking, and no other turn in that conversation could
// proceed. The framework's default is unbounded, which is not a safe default
// for a shared multi-tenant runtime.
//
// Four is conservative on purpose. The registered tools answer in one round; a
// model that calls both, then retries once after a bad-argument error, needs
// three. Exceeding the bound ends the run with an error rather than truncating
// the answer silently, so this is a backstop, not a budget to spend.
const maxToolIterations = 4

type runtimeHTTPAdapter interface {
	Handler() http.Handler
	Close() error
}

// Runtime owns the process-local Agent, Runner and protocol adapter objects for
// one immutable agent revision. Conversation state remains in SessionService.
// Runner is the real Runner and the only one Close releases; protocol adapters
// receive a wrapper that supplies the authenticated identity instead.
type Runtime struct {
	TenantID       string
	AgentAppID     string
	RevisionID     string
	AppName        string
	ModelName      string
	Agent          trpcagent.Agent
	Runner         runner.Runner
	SessionService session.Service

	openAI         runtimeHTTPAdapter
	openAIHandler  http.Handler
	protocolRunner runner.Runner
	closeOnce      sync.Once
	closeErr       error

	// store is this Runtime's claim on the storage it runs against, held from
	// the moment it is built until Close releases it.
	//
	// It is a Lease rather than a boolean because what has to happen at Close
	// is not a property of this Runtime at all: a shared process store is
	// borrowed and must survive, a store built for one profile is reference
	// counted by its Router, and a store this Runtime was handed outright must
	// be closed exactly once. A flag could only ever encode the last of those,
	// and every value that is not a flag — a close hook, a second owner — is a
	// second closing path that does not know about the first.
	store storagebundle.Lease
}

// NewDemoRuntime builds a real tRPC-Agent-Go LLMAgent and Runner without
// requiring an external model API. It stays on the deterministic provider so
// the bootstrap path needs no endpoint and no credential; a revision selects a
// real model through its own ModelConfig.
//
// It is built under an authorizer that entitles nothing, which is not a
// weakening of the demo but a statement about it: the demo config names no
// secret_ref and no policy_refs, so it needs no entitlement, and building it
// this way proves that the capability-free path stays open with no security
// configuration behind it.
func NewDemoRuntime() *Runtime {
	sessionService := sessioninmemory.NewSessionService()
	config := tenant.RevisionConfig{
		AgentName:   DemoAgentName,
		Description: "Deterministic bootstrap agent",
		Instruction: "Return the model response. This bootstrap runtime verifies the service path.",
		Model: tenant.ModelConfig{
			Provider: ProviderDeterministic,
			Name:     DemoModelName,
		},
	}
	// The digest is computed rather than left empty. A published revision now
	// has its digest re-verified when it is built, so a helper that hand-built
	// one without a digest would be a helper that produces revisions the
	// platform refuses — and the first place that would surface is a demo that
	// used to work.
	digest, err := config.Digest()
	if err != nil {
		_ = sessionService.Close()
		panic(fmt.Sprintf("agent: digest static demo revision: %v", err))
	}
	revision := tenant.AgentRevision{
		ID:           "echo-v1",
		TenantID:     "demo",
		AgentAppID:   "echo",
		Status:       tenant.RevisionStatusPublished,
		Config:       config,
		ConfigDigest: digest,
	}
	runtime, err := newOwnedRuntime(
		revision, sessionService, nil, security.DenyCapabilities())
	if err != nil {
		// No close here: newOwnedRuntime took the session service on call and
		// has already released it.
		panic(fmt.Sprintf("agent: build static demo runtime: %v", err))
	}
	return runtime
}

// NewRuntime builds the currently supported runtime from an immutable published
// revision, on the storage that revision names.
//
// stores decides what revision.Config.BackendProfileID means. It is not
// optional and there is no default: which storage a tenant's conversations land
// in is not a decision that may be made by omission, and a Runtime that ignored
// a profile reference would be serving a revision the platform never agreed to
// serve.
//
// ctx bounds the storage resolution only. The Runtime that comes back is not
// tied to it and outlives it; it is released by Close.
//
// authorizer is mandatory for the same reason and in the same way. It is not
// variadic and there is no allow-everything convenience value: every call site
// states it, and the compiler makes sure a new one does too. A caller with no
// capability configuration passes security.DenyCapabilities().
func NewRuntime(
	ctx context.Context,
	revision tenant.AgentRevision,
	stores storagebundle.Resolver,
	authorizer security.RevisionAuthorizer,
) (*Runtime, error) {
	return newRuntime(ctx, revision, stores, nil, authorizer)
}

// NewRuntimeFromRevision builds a Runtime on one session service the caller
// already owns and keeps owning.
//
// It is the entry point for callers that hold a process store and have no way
// to build another one. Because it cannot honour a backend profile reference,
// it refuses one: a revision whose BackendProfileID is set comes back as
// storagebundle.ErrProfileNotFound rather than being served by this store. That
// is a behaviour change and the intended one — ignoring the reference is how a
// tenant's conversations end up in storage its revision did not name.
func NewRuntimeFromRevision(
	revision tenant.AgentRevision,
	sessionService session.Service,
	authorizer security.RevisionAuthorizer,
) (*Runtime, error) {
	return newRuntime(
		context.Background(),
		revision,
		storagebundle.Fixed(storagebundle.Bundle{Session: sessionService}),
		nil,
		authorizer,
	)
}

// newRuntime builds a Runtime against resolved storage. A nil auditSink selects
// the process default, which is slog; it is a parameter so the tool trail can be
// read back without redirecting the logger of the whole process.
func newRuntime(
	ctx context.Context,
	revision tenant.AgentRevision,
	stores storagebundle.Resolver,
	auditSink tool.AuditSink,
	authorizer security.RevisionAuthorizer,
) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("agent: context is required")
	}
	if stores == nil {
		return nil, fmt.Errorf("agent: storage resolver is required")
	}
	plan, err := planRuntime(revision, auditSink, authorizer)
	if err != nil {
		return nil, err
	}
	// Storage is acquired here and nowhere earlier. Everything above this line
	// is a refusal the platform owes without touching anything: a revision that
	// is not published, not intact or not entitled must not have caused a
	// connection, a table or a credential read on its way to being refused.
	store, err := stores.Resolve(
		ctx,
		tenant.TenantContext{TenantID: revision.TenantID},
		revision.Config.BackendProfileID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"agent: resolve session store for revision %q: %w", revision.ID, err)
	}
	return assembleRuntime(plan, store)
}

// newOwnedRuntime builds a Runtime that owns sessions outright.
//
// Ownership transfers on call and not on success: an error here has already
// released sessions, so no caller is left holding a store it has to guess about
// — and none of them can close it twice.
func newOwnedRuntime(
	revision tenant.AgentRevision,
	sessions session.Service,
	auditSink tool.AuditSink,
	authorizer security.RevisionAuthorizer,
) (*Runtime, error) {
	store := storagebundle.Own(storagebundle.Bundle{Session: sessions})
	plan, err := planRuntime(revision, auditSink, authorizer)
	if err != nil {
		return nil, errors.Join(err, store.Release())
	}
	return assembleRuntime(plan, store)
}

// runtimePlan is everything a Runtime needs that can be decided before it holds
// any storage. Building it acquires nothing and closes nothing, so a plan that
// fails leaves nothing behind.
type runtimePlan struct {
	revision tenant.AgentRevision
	appName  string
	agent    trpcagent.Agent
}

// planRuntime checks a revision and builds the agent it describes.
//
// The order of the checks below is the security property, not a style:
//
//   - Identity and config shape first, because nothing else is meaningful
//     without them.
//   - The digest next. A published revision is immutable, so its config must
//     still hash to the value recorded when it was created; anything else means
//     the stored row was edited outside a Repository, and the edit is exactly
//     how a secret_ref or a base_url would be moved after review.
//   - The authorizer next, and before the tool registry, any secret resolution
//     and any storage. A revision its tenant is not entitled to must be refused
//     without the platform revealing whether the policy exists or whether the
//     environment variable does — and without reading the variable at all.
func planRuntime(
	revision tenant.AgentRevision,
	auditSink tool.AuditSink,
	authorizer security.RevisionAuthorizer,
) (runtimePlan, error) {
	if revision.Status != tenant.RevisionStatusPublished {
		return runtimePlan{}, fmt.Errorf("agent: revision %q is not published", revision.ID)
	}
	if err := (tenant.TenantContext{TenantID: revision.TenantID}).Validate(); err != nil {
		return runtimePlan{}, fmt.Errorf("agent: invalid runtime tenant: %w", err)
	}
	if err := tenant.ValidateResourceID("app id", revision.AgentAppID); err != nil {
		return runtimePlan{}, fmt.Errorf("agent: invalid runtime app: %w", err)
	}
	if err := tenant.ValidateResourceID("revision id", revision.ID); err != nil {
		return runtimePlan{}, fmt.Errorf("agent: invalid runtime revision: %w", err)
	}
	digest, err := revision.Config.Digest()
	if err != nil {
		return runtimePlan{}, fmt.Errorf("agent: invalid revision config: %w", err)
	}
	// An empty stored digest is a mismatch, not an exemption. A revision that
	// carries no fingerprint cannot be checked against one, and "unverifiable"
	// has to fail the same way "wrong" does.
	if revision.ConfigDigest == "" || revision.ConfigDigest != digest {
		return runtimePlan{}, fmt.Errorf(
			"agent: revision %q: %w", revision.ID, tenant.ErrConfigIntegrity)
	}
	if authorizer == nil {
		return runtimePlan{}, fmt.Errorf("agent: revision authorizer is required")
	}
	if err := authorizer.AuthorizeRevision(revision.TenantID, revision.Config); err != nil {
		// Wrapped with the revision id and nothing else. The refusal must read
		// the same whichever reference caused it.
		return runtimePlan{}, fmt.Errorf("agent: revision %q: %w", revision.ID, err)
	}
	// Tools and policies are resolved before anything is constructed. A
	// revision the platform cannot authorize must not reach a model, a Runner
	// or a protocol adapter — and running this before the model also means an
	// unbuildable revision never causes a credential to be resolved.
	tools, err := tool.Builtin().Resolve(revision.Config.ToolRefs, revision.Config.PolicyRefs)
	if err != nil {
		return runtimePlan{}, fmt.Errorf(
			"agent: assemble tools for revision %q: %w", revision.ID, err)
	}
	llmModel, err := newModel(revision.Config.Model)
	if err != nil {
		return runtimePlan{}, fmt.Errorf(
			"agent: build model for revision %q: %w", revision.ID, err)
	}
	options := []llmagent.Option{
		llmagent.WithModel(llmModel),
		llmagent.WithDescription(revision.Config.Description),
		llmagent.WithInstruction(revision.Config.Instruction),
		llmagent.WithGenerationConfig(generationConfig(revision.Config.Model)),
	}
	// A revision with no tools is built exactly as before. The tool options are
	// not merely inert without tools — the iteration bound and the callbacks
	// change agent behavior — so a revision that never asked for tools does not
	// get them.
	if len(tools) > 0 {
		options = append(
			options,
			llmagent.WithTools(tools),
			llmagent.WithMaxToolIterations(maxToolIterations),
			llmagent.WithToolCallbacks(tool.NewAuditCallbacks(auditSink)),
		)
	}
	return runtimePlan{
		revision: revision,
		appName:  fmt.Sprintf("t/%s/a/%s", revision.TenantID, revision.AgentAppID),
		agent:    llmagent.New(revision.Config.AgentName, options...),
	}, nil
}

// assembleRuntime builds the Runner and the protocol adapter over a store this
// Runtime now holds.
//
// Every failure from here on releases that store. This is the half of the build
// that owns something, so it is also the half that has to hand it back — a
// leaked lease is not a leaked object, it is a Router.Close that never returns.
func assembleRuntime(plan runtimePlan, store storagebundle.Lease) (*Runtime, error) {
	revision := plan.revision
	if store == nil {
		return nil, fmt.Errorf(
			"agent: storage resolver returned no lease for revision %q", revision.ID)
	}
	sessions := store.Bundle().Session
	if sessions == nil {
		return nil, errors.Join(
			fmt.Errorf("agent: session service is required"),
			store.Release(),
		)
	}
	runtime := &Runtime{
		TenantID:       revision.TenantID,
		AgentAppID:     revision.AgentAppID,
		RevisionID:     revision.ID,
		AppName:        plan.appName,
		ModelName:      revision.Config.Model.Name,
		Agent:          plan.agent,
		Runner:         runner.NewRunner(plan.appName, plan.agent, runner.WithSessionService(sessions)),
		SessionService: sessions,
		store:          store,
	}
	// The adapter reads userID and sessionID from the request payload, so it
	// receives the identity-enforcing wrapper instead of the real Runner.
	runtime.protocolRunner = &contextRunner{
		inner:      runtime.Runner,
		tenantID:   revision.TenantID,
		appID:      revision.AgentAppID,
		revisionID: revision.ID,
	}
	openAI, err := openaiserver.New(
		openaiserver.WithRunner(runtime.protocolRunner),
		openaiserver.WithSessionService(runtime.SessionService),
		openaiserver.WithAppName(runtime.AppName),
		openaiserver.WithModelName(runtime.ModelName),
		openaiserver.WithBasePath("/v1"),
	)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("agent: create OpenAI adapter for revision %q: %w", revision.ID, err),
			runtime.Close(),
		)
	}
	runtime.openAI = openAI
	runtime.openAIHandler = openAI.Handler()
	if runtime.openAIHandler == nil {
		return nil, errors.Join(
			fmt.Errorf("agent: OpenAI adapter for revision %q has no handler", revision.ID),
			runtime.Close(),
		)
	}
	return runtime, nil
}

// OpenAIHandler returns the single protocol handler owned by this Runtime. It
// is resolved once at build time, so callers must not cache adapters per
// Runtime pointer.
func (r *Runtime) OpenAIHandler() (http.Handler, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return r.openAIHandler, nil
}

func (r *Runtime) validate() error {
	if r == nil {
		return fmt.Errorf("agent: runtime is nil")
	}
	if r.TenantID == "" || r.AgentAppID == "" || r.RevisionID == "" {
		return fmt.Errorf("agent: runtime identity is incomplete")
	}
	if r.AppName == "" || r.ModelName == "" || r.Agent == nil || r.Runner == nil ||
		r.SessionService == nil || r.openAI == nil || r.openAIHandler == nil {
		return fmt.Errorf("agent: runtime execution unit is incomplete")
	}
	// The storage claim, checked separately from the execution unit because it
	// is not another part of one. A Runtime with every other field set runs
	// perfectly well — right up until whoever does hold that store closes it,
	// because nothing was counting this Runtime as a holder. Close is the only
	// thing that releases a lease, so a Runtime that never had one is a Runtime
	// whose storage lifetime nothing bounds.
	if r.store == nil {
		return fmt.Errorf("agent: runtime holds no storage lease")
	}
	return nil
}

func (r *Runtime) validateFor(revision tenant.AgentRevision) error {
	if err := r.validate(); err != nil {
		return err
	}
	if r.TenantID != revision.TenantID || r.AgentAppID != revision.AgentAppID ||
		r.RevisionID != revision.ID {
		return fmt.Errorf(
			"agent: runtime identity %q/%q/%q does not match revision %q/%q/%q",
			r.TenantID,
			r.AgentAppID,
			r.RevisionID,
			revision.TenantID,
			revision.AgentAppID,
			revision.ID,
		)
	}
	return nil
}

// Close releases the protocol adapter, the Runner, and this Runtime's claim on
// its storage, in that order. It is safe for concurrent use and idempotent:
// every call returns the error produced by the first close.
//
// The store goes last because the two above it may still be writing to it: a
// Runner that is stopping flushes the turn it was serving, and a session
// service released before that flush would drop it.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		var closeErr error
		if r.openAI != nil {
			closeErr = errors.Join(closeErr, r.openAI.Close())
		}
		if r.Runner != nil {
			closeErr = errors.Join(closeErr, r.Runner.Close())
		}
		if r.store != nil {
			closeErr = errors.Join(closeErr, r.store.Release())
		}
		r.closeErr = closeErr
	})
	return r.closeErr
}

type deterministicModel struct {
	name string
}

func (m deterministicModel) Info() model.Info {
	return model.Info{Name: m.name, ContextWindow: 4096}
}

func (m deterministicModel) GenerateContent(
	ctx context.Context,
	request *model.Request,
) (<-chan *model.Response, error) {
	if request == nil {
		return nil, fmt.Errorf("deterministic model: nil request")
	}
	lastContent := lastUserContent(request.Messages)
	content := "echo: " + lastContent
	responses := make(chan *model.Response, 3)
	go func() {
		defer close(responses)
		if request.Stream {
			if !sendResponse(ctx, responses, &model.Response{
				Object:    model.ObjectTypeChatCompletionChunk,
				Model:     m.name,
				IsPartial: true,
				Choices: []model.Choice{{
					Index: 0,
					Delta: model.Message{Role: model.RoleAssistant, Content: "echo: "},
				}},
			}) {
				return
			}
			if !sendResponse(ctx, responses, &model.Response{
				Object:    model.ObjectTypeChatCompletionChunk,
				Model:     m.name,
				IsPartial: true,
				Choices: []model.Choice{{
					Index: 0,
					Delta: model.Message{Content: lastContent},
				}},
			}) {
				return
			}
		}
		_ = sendResponse(ctx, responses, &model.Response{
			Object: model.ObjectTypeChatCompletion,
			Model:  m.name,
			Done:   true,
			Choices: []model.Choice{{
				Index:   0,
				Message: model.NewAssistantMessage(content),
			}},
		})
	}()
	return responses, nil
}

func lastUserContent(messages []model.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == model.RoleUser {
			return messages[i].Content
		}
	}
	return ""
}

func sendResponse(
	ctx context.Context,
	responses chan<- *model.Response,
	response *model.Response,
) bool {
	select {
	case <-ctx.Done():
		return false
	case responses <- response:
		return true
	}
}
