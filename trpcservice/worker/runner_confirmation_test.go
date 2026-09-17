package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	checkpointmemory "trpc.group/trpc-go/trpc-agent-go/graph/checkpoint/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsessionmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestGraphContinuationReferenceIsStrictAndRoundTrips(t *testing.T) {
	want := graphContinuationCoordinate{SchemaVersion: 1, Kind: "graph_tool_confirmation", LineageID: "lineage",
		CheckpointID: "checkpoint", Namespace: "workflow", TaskID: "call-1", ToolCallID: "call-1", ToolName: "danger"}
	encoded, err := encodeGraphContinuationRef(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeGraphContinuationRef(encoded)
	if err != nil || got != want {
		t.Fatalf("coordinate=%#v err=%v", got, err)
	}
	tampered, err := json.Marshal(map[string]any{"schema_version": 1, "kind": "graph_tool_confirmation", "lineage_id": "lineage",
		"checkpoint_id": "checkpoint", "namespace": "workflow", "task_id": "call-1", "tool_call_id": "call-1",
		"tool_name": "danger", "unexpected": true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGraphContinuationRef(graphContinuationRefPrefix + base64.RawURLEncoding.EncodeToString(tampered)); err == nil {
		t.Fatal("unknown graph continuation field accepted")
	}
	want.TaskID = "graph-task"
	encoded, err = encodeGraphContinuationRef(want)
	if err != nil {
		t.Fatalf("independent graph task id rejected: %v", err)
	}
	got, err = decodeGraphContinuationRef(encoded)
	if err != nil || got != want {
		t.Fatalf("independent task coordinate=%#v err=%v", got, err)
	}
}

func TestConfirmationBindingIDUsesFrozenReplyRouteForLegacyTextPayload(t *testing.T) {
	payloads := messagingmemory.New()
	if err := payloads.PutReplyRoute(messaging.ReplyRoute{TenantID: "tenant-a", RequestID: "request-a", Channel: "webui",
		ChannelBindingID: "binding-a", ExternalAccountID: "account-a", ConfigVersion: 1}); err != nil {
		t.Fatal(err)
	}
	bindingID, err := confirmationBindingID(context.Background(), payloads, "tenant-a", "request-a", []byte(`{"text":"legacy"}`))
	if err != nil || bindingID != "binding-a" {
		t.Fatalf("binding_id=%q err=%v", bindingID, err)
	}
	if _, err := confirmationBindingID(context.Background(), payloads, "tenant-a", "request-a", []byte(`{"channel_binding_id":"other","text":"bad"}`)); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("mismatched binding err=%v", err)
	}
}

type confirmationModel struct{ calls int }

func (m *confirmationModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.calls++
	response := &model.Response{ID: "confirmation-model", Object: model.ObjectTypeChatCompletion, Done: true,
		Usage: &model.Usage{PromptTokens: 3, CompletionTokens: 2}, Choices: []model.Choice{{Index: 0}}}
	if len(request.Messages) != 0 && request.Messages[len(request.Messages)-1].Role == model.RoleTool {
		response.Choices[0].Message = model.NewAssistantMessage("confirmed-result")
	} else {
		response.Choices[0].Message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "call-danger", Type: "function",
			Function: model.FunctionDefinitionParam{Name: "danger", Arguments: []byte(`{"value":1}`)}}}}
	}
	values := make(chan *model.Response, 1)
	values <- response
	close(values)
	return values, nil
}
func (*confirmationModel) Info() model.Info { return model.Info{Name: "confirmation-model"} }

type dangerousTool struct{ calls int }

func (*dangerousTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{Name: "danger", InputSchema: &agenttool.Schema{Type: "object"}}
}
func (t *dangerousTool) Call(context.Context, []byte) (any, error) {
	t.calls++
	return map[string]any{"done": true}, nil
}

type oneToolResolver struct{ value agenttool.Tool }

func (r oneToolResolver) ResolveTools(_ context.Context, _ string, refs []profile.VersionedRef) ([]agenttool.Tool, error) {
	if len(refs) != 1 || refs[0].ID != "danger" || refs[0].Version != 1 {
		return nil, runtime.ErrVersionMismatch
	}
	return []agenttool.Tool{r.value}, nil
}

type staticCheckpointResolver struct{ value graph.CheckpointSaver }

func (r staticCheckpointResolver) ResolveCheckpointSaver(context.Context, string, string, int64) (graph.CheckpointSaver, error) {
	if r.value == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return r.value, nil
}

type confirmationRunGuard struct {
	policy governance.PolicySnapshot
	usage  governance.Usage
}

func (g *confirmationRunGuard) Begin(_ context.Context, envelope runtime.ExecutionEnvelope, modelRef governance.VersionedRef, _ []byte) (governance.RunPermit, error) {
	return governance.RunPermit{Policy: g.policy, Model: modelRef, Decision: governance.Decision{Action: governance.ActionAllow},
		Reservation: governance.Reservation{ReservationID: "reservation", TenantID: envelope.TenantID, RequestID: envelope.RequestID, State: governance.ReservationReserved, Version: 1}}, nil
}
func (g *confirmationRunGuard) Finish(_ context.Context, _ governance.RunPermit, usage governance.Usage, _ []byte) (governance.Decision, error) {
	g.usage = usage
	return governance.Decision{Action: governance.ActionAllow}, nil
}
func (*confirmationRunGuard) Refund(context.Context, governance.RunPermit, string) error { return nil }
func (*confirmationRunGuard) Abort(context.Context, runtime.ExecutionEnvelope, governance.VersionedRef, string) error {
	return nil
}
func (*confirmationRunGuard) Record(context.Context, governance.Decision) error { return nil }

type memoryConfirmationCoordinator struct {
	mu           sync.Mutex
	sessions     sessionstore.AtomicSessionStore
	confirmation governance.Confirmation
	grant        governance.Grant
	attempt      governance.ToolAttempt
}

func (c *memoryConfirmationCoordinator) Suspend(ctx context.Context, commit sessionstore.CommitTurnRequest, in governance.SuspensionRequest) (governance.Confirmation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.confirmation.ConfirmationID != "" {
		return c.confirmation, nil
	}
	if _, err := c.sessions.CommitTurn(ctx, commit); err != nil {
		return governance.Confirmation{}, err
	}
	c.confirmation = governance.Confirmation{SuspensionRequest: in, State: governance.ConfirmationPending, Version: 1}
	return c.confirmation, nil
}
func (c *memoryConfirmationCoordinator) Decide(_ context.Context, in governance.ConfirmationDecision) (governance.Confirmation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.confirmation.State != governance.ConfirmationPending || in.ExpectedVersion != c.confirmation.Version {
		return governance.Confirmation{}, runtime.ErrVersionConflict
	}
	c.confirmation.State, c.confirmation.Version, c.confirmation.DecisionAt = governance.ConfirmationApproved, 2, in.DecidedAt
	c.grant = governance.Grant{GrantID: "grant-danger", TenantID: c.confirmation.TenantID, ConfirmationID: c.confirmation.ConfirmationID,
		RequestID: c.confirmation.RequestID, SubjectID: c.confirmation.SubjectID, Tool: c.confirmation.Tool, ToolCallID: c.confirmation.ToolCallID,
		ArgsDigest: c.confirmation.ArgsDigest, PolicyVersion: c.confirmation.PolicyVersion, Version: 1}
	return c.confirmation, nil
}
func (*memoryConfirmationCoordinator) ExpireDue(context.Context, time.Time, int) ([]governance.Confirmation, error) {
	return nil, nil
}
func (c *memoryConfirmationCoordinator) GetConfirmation(_ context.Context, tenantID, id string) (governance.Confirmation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.confirmation.TenantID != tenantID || c.confirmation.ConfirmationID != id {
		return governance.Confirmation{}, runtime.ErrNotFound
	}
	return c.confirmation, nil
}
func (c *memoryConfirmationCoordinator) GetConfirmationByRequest(_ context.Context, tenantID, requestID string) (governance.Confirmation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.confirmation.TenantID != tenantID || c.confirmation.RequestID != requestID {
		return governance.Confirmation{}, runtime.ErrNotFound
	}
	return c.confirmation, nil
}
func (c *memoryConfirmationCoordinator) GetGrantByConfirmation(_ context.Context, tenantID, confirmationID string) (governance.Grant, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.grant.TenantID != tenantID || c.grant.ConfirmationID != confirmationID {
		return governance.Grant{}, runtime.ErrNotFound
	}
	return c.grant, nil
}
func (c *memoryConfirmationCoordinator) ConsumeGrant(_ context.Context, in governance.GrantClaim) (governance.Grant, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.confirmation.State != governance.ConfirmationApproved || c.grant.GrantID != in.GrantID || c.grant.Version != in.ExpectedVersion {
		return governance.Grant{}, runtime.ErrVersionConflict
	}
	c.confirmation.State = governance.ConfirmationConsumed
	c.confirmation.Version++
	c.grant.Version++
	c.grant.ConsumedAt = time.Now().UTC()
	c.attempt = governance.ToolAttempt{TenantID: in.TenantID, GrantID: in.GrantID, RequestID: in.RequestID, ToolCallID: in.ToolCallID, State: governance.ToolAttemptEffectUnknown}
	return c.grant, nil
}
func (c *memoryConfirmationCoordinator) GetToolAttempt(_ context.Context, tenantID, grantID string) (governance.ToolAttempt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.attempt.TenantID != tenantID || c.attempt.GrantID != grantID {
		return governance.ToolAttempt{}, runtime.ErrNotFound
	}
	return c.attempt, nil
}
func (c *memoryConfirmationCoordinator) FinishToolAttempt(_ context.Context, in governance.FinishToolAttemptRequest) (governance.ToolAttempt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.attempt.State != governance.ToolAttemptEffectUnknown {
		return governance.ToolAttempt{}, runtime.ErrVersionConflict
	}
	c.attempt.State, c.attempt.ResultRef = in.State, in.ResultRef
	return c.attempt, nil
}

func TestRunnerDangerousToolSuspendsAndResumesExactlyOnce(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1, RequestID: "confirmed-request", SessionID: "session",
		UserID: "user", Channel: "fake", InputSeq: 1, PayloadRef: "payload://confirmed", CreatedAt: time.Now().UTC()}
	key := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1, AgentAppRevision: 1, ContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1}
	snapshot := profile.ExecutionProfileSnapshot{Key: key, TenantVersion: 1, AgentAppVersion: 1, ContentDigest: "digest", AppName: "tenant-a/app",
		AgentKind: agentapp.AgentKindLLM, ModelProfileRef: profile.VersionedRef{ID: "model", Version: 1}, ToolRefs: []profile.VersionedRef{{ID: "danger", Version: 1}}}
	profiles := profilememory.NewResolver(snapshot)
	policyValue := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, AllowedModels: []governance.VersionedRef{{ID: "model", Version: 1}},
		InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled, Tools: []governance.ToolRule{{ToolID: "danger", Version: 1, Dangerous: true, ConfirmationSupported: true}}}
	digest, _, _ := governance.PolicyDigest(policyValue)
	policy := governance.PolicySnapshot{TenantID: envelope.TenantID, Version: 1, SchemaVersion: 1, Policy: policyValue, ContentDigest: digest, PublishedAt: time.Now().UTC()}
	policies := governancememory.New(0, 0)
	if err := policies.PublishPolicy(policy); err != nil {
		t.Fatal(err)
	}
	payloads := messagingmemory.New()
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: envelope.TenantID, RequestID: envelope.RequestID,
		PayloadRef: envelope.PayloadRef, ContentDigest: "digest", Content: []byte(`{"text":"do it","channel_binding_id":"binding"}`), KeyVersion: 7}); err != nil {
		t.Fatal(err)
	}
	sessions := sessionmemory.New()
	coordinator := &memoryConfirmationCoordinator{sessions: sessions}
	modelValue := &confirmationModel{}
	toolValue := &dangerousTool{}
	factory := serviceagent.Factory{Profiles: profiles, Models: staticModelResolver{model: modelValue}, Tools: oneToolResolver{value: toolValue}, Policies: policies,
		Confirmations: coordinator, ToolResults: payloads}
	bundles := profilememory.NewBundleManager(func(ctx context.Context, requested profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		resolved, err := profiles.Resolve(ctx, requested)
		if err != nil {
			return nil, nil, err
		}
		root, err := factory.Build(ctx, resolved)
		return &serviceagent.Bundle{AppName: resolved.AppName, Root: root}, nil, err
	})
	guard := &confirmationRunGuard{policy: policy}
	executor := RunnerExecutor{Tasks: taskStub{envelope: envelope}, Profiles: profiles, Bundles: bundles, Sessions: sessions, SDKSessions: agentsessionmemory.NewSessionService(), Payloads: payloads,
		Inputs:     JSONTextInputDecoder{},
		Governance: guard, Confirmations: coordinator, ContinuationTools: factory}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 1, nil); err != nil {
		t.Fatal(err)
	}
	confirmation, err := coordinator.GetConfirmationByRequest(context.Background(), envelope.TenantID, envelope.RequestID)
	if err != nil || confirmation.State != governance.ConfirmationPending || toolValue.calls != 0 {
		t.Fatalf("confirmation=%#v tool_calls=%d err=%v", confirmation, toolValue.calls, err)
	}
	promptRef := "confirmation://" + envelope.TenantID + "/" + confirmation.ConfirmationID
	prompt, err := messaging.ResolveReplyContent(context.Background(), payloads, envelope.TenantID, envelope.RequestID, promptRef)
	if err != nil || len(prompt.Content) == 0 {
		t.Fatalf("prompt=%#v err=%v", prompt, err)
	}
	_, outbox, _ := sessions.SnapshotEffects(sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID})
	foundReply := false
	for _, value := range outbox {
		foundReply = foundReply || value.Kind == "reply" && value.PayloadRef == promptRef
	}
	if !foundReply {
		t.Fatalf("confirmation reply outbox missing: %#v", outbox)
	}
	if _, err := coordinator.Decide(context.Background(), governance.ConfirmationDecision{TenantID: envelope.TenantID, ConfirmationID: confirmation.ConfirmationID, SubjectID: envelope.UserID, Approve: true, ExpectedVersion: 1, DecidedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 2, nil); err != nil {
		t.Fatal(err)
	}
	if toolValue.calls != 1 || modelValue.calls != 2 || guard.usage.InputTokens != 6 || guard.usage.OutputTokens != 4 {
		t.Fatalf("tool=%d model=%d usage=%#v", toolValue.calls, modelValue.calls, guard.usage)
	}
	result, err := payloads.GetResult(context.Background(), envelope.TenantID, envelope.RequestID)
	if err != nil || string(result.Content) != "confirmed-result" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 3, nil); err != nil && !errors.Is(err, runtime.ErrAlreadyTerminal) {
		t.Fatal(err)
	}
	if toolValue.calls != 1 {
		t.Fatalf("dangerous tool replayed: %d", toolValue.calls)
	}
	terminal, err := sessions.GetTerminalByInputSeq(context.Background(), sessionstore.TerminalKey{SessionKey: sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}, InputSeq: 1})
	if err != nil || terminal.Outcome != runtime.OutcomeSucceeded {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
}

func TestRunnerGraphDangerousToolResumesFromCheckpointExactlyOnce(t *testing.T) {
	const childDigest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	envelope := runtime.ExecutionEnvelope{SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "graph-app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "graph-digest", ConfigVersion: 1, PolicyVersion: 1, RequestID: "graph-confirmed-request",
		SessionID: "graph-session", UserID: "user", Channel: "fake", InputSeq: 1, PayloadRef: "payload://graph-confirmed", CreatedAt: time.Now().UTC()}
	rootKey := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: 1, AgentAppID: envelope.AgentAppID,
		AgentAppVersion: 1, AgentAppRevision: 1, ContentDigest: envelope.AgentContentDigest, ConfigVersion: 1, PolicyVersion: 1}
	childKey := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: 1, AgentAppID: "child", AgentAppRevision: 1,
		ContentDigest: childDigest, ConfigVersion: 1, PolicyVersion: 1}
	node := agentapp.AgentNodeSpecV1{Key: "worker", FailurePolicy: agentapp.FailurePolicyFailFast,
		AgentRef: agentapp.PublishedAgentRef{AgentAppID: "child", Revision: 1, ContentDigest: childDigest}}
	root := profile.ExecutionProfileSnapshot{Key: rootKey, TenantVersion: 1, AgentAppVersion: 1, ContentDigest: envelope.AgentContentDigest,
		AppName: "tenant-a/graph-app", AgentKind: agentapp.AgentKindGraph,
		AgentSpec: agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{node}, EntryNode: "worker", MaxConcurrency: 1,
			Checkpoint: agentapp.CheckpointPolicyV1{Required: true, Namespace: "workflow"}}}
	child := profile.ExecutionProfileSnapshot{Key: childKey, TenantVersion: 1, ContentDigest: childDigest, AppName: "tenant-a/child",
		AgentKind: agentapp.AgentKindLLM, Instruction: "use the tool", ModelProfileRef: profile.VersionedRef{ID: "model", Version: 1},
		ToolRefs: []profile.VersionedRef{{ID: "danger", Version: 1}}}
	profiles := profilememory.NewResolver(root, child)
	policyValue := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow,
		AllowedModels: []governance.VersionedRef{{ID: "model", Version: 1}}, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled,
		Tools: []governance.ToolRule{{ToolID: "danger", Version: 1, Dangerous: true, ConfirmationSupported: true}}}
	digest, _, _ := governance.PolicyDigest(policyValue)
	policy := governance.PolicySnapshot{TenantID: envelope.TenantID, Version: 1, SchemaVersion: 1, Policy: policyValue,
		ContentDigest: digest, PublishedAt: time.Now().UTC()}
	policies := governancememory.New(0, 0)
	if err := policies.PublishPolicy(policy); err != nil {
		t.Fatal(err)
	}
	payloads := messagingmemory.New()
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: envelope.TenantID, RequestID: envelope.RequestID,
		PayloadRef: envelope.PayloadRef, ContentDigest: "digest", Content: []byte(`{"text":"do it","channel_binding_id":"binding"}`), KeyVersion: 7}); err != nil {
		t.Fatal(err)
	}
	sessions := sessionmemory.New()
	coordinator := &memoryConfirmationCoordinator{sessions: sessions}
	modelValue := &confirmationModel{}
	toolValue := &dangerousTool{}
	factory := serviceagent.Factory{Profiles: profiles, Models: staticModelResolver{model: modelValue}, Tools: oneToolResolver{value: toolValue},
		Policies: policies, Confirmations: coordinator, ToolResults: payloads,
		Checkpoints: staticCheckpointResolver{value: checkpointmemory.NewSaver()}}
	bundles := profilememory.NewBundleManager(func(ctx context.Context, requested profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		resolved, err := profiles.Resolve(ctx, requested)
		if err != nil {
			return nil, nil, err
		}
		built, err := factory.Build(ctx, resolved)
		return &serviceagent.Bundle{AppName: resolved.AppName, Root: built}, nil, err
	})
	guard := &confirmationRunGuard{policy: policy}
	executor := RunnerExecutor{Tasks: taskStub{envelope: envelope}, Profiles: profiles, Bundles: bundles, Sessions: sessions, SDKSessions: agentsessionmemory.NewSessionService(), Payloads: payloads,
		Inputs: JSONTextInputDecoder{}, Governance: guard, Confirmations: coordinator, ContinuationTools: factory}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 1, nil); err != nil {
		t.Fatal(err)
	}
	confirmation, err := coordinator.GetConfirmationByRequest(context.Background(), envelope.TenantID, envelope.RequestID)
	if err != nil || confirmation.State != governance.ConfirmationPending || toolValue.calls != 0 ||
		!strings.HasPrefix(confirmation.CheckpointRef, graphContinuationRefPrefix) {
		t.Fatalf("confirmation=%#v tool_calls=%d err=%v", confirmation, toolValue.calls, err)
	}
	if _, err := coordinator.Decide(context.Background(), governance.ConfirmationDecision{TenantID: envelope.TenantID,
		ConfirmationID: confirmation.ConfirmationID, SubjectID: envelope.UserID, Approve: true, ExpectedVersion: 1, DecidedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	// Simulate a Worker crash after the guarded external effect and encrypted
	// result commit, but before the Graph checkpoint is resumed.
	grant, err := coordinator.GetGrantByConfirmation(context.Background(), envelope.TenantID, confirmation.ConfirmationID)
	if err != nil {
		t.Fatal(err)
	}
	callable, err := factory.ResolveConfirmedTool(context.Background(), envelope.TenantID, profile.VersionedRef{ID: confirmation.Tool.ID, Version: confirmation.Tool.Version})
	if err != nil {
		t.Fatal(err)
	}
	crashContext := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: envelope.TenantID,
		RequestID: envelope.RequestID, SubjectID: envelope.UserID, PolicyVersion: envelope.PolicyVersion, GrantID: grant.GrantID,
		GrantVersion: grant.Version, ToolCallID: confirmation.ToolCallID, ArgsDigest: confirmation.ArgsDigest, PayloadKeyVersion: 7})
	if _, err := callable.Call(crashContext, []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	if toolValue.calls != 1 || coordinator.confirmation.State != governance.ConfirmationConsumed {
		t.Fatalf("crash seam not established: tool=%d confirmation=%#v", toolValue.calls, coordinator.confirmation)
	}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 2, nil); err != nil {
		t.Fatal(err)
	}
	if toolValue.calls != 1 || modelValue.calls != 2 {
		t.Fatalf("tool=%d model=%d", toolValue.calls, modelValue.calls)
	}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 3, nil); err != nil && !errors.Is(err, runtime.ErrAlreadyTerminal) {
		t.Fatal(err)
	}
	if toolValue.calls != 1 {
		t.Fatalf("graph tool replayed: %d", toolValue.calls)
	}
	terminal, err := sessions.GetTerminalByInputSeq(context.Background(), sessionstore.TerminalKey{SessionKey: sessionstore.SessionKey{
		TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}, InputSeq: 1})
	if err != nil || terminal.Outcome != runtime.OutcomeSucceeded {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
}

func TestRunnerGraphDeniedConfirmationTerminalizesWithoutResuming(t *testing.T) {
	const childDigest = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	envelope := runtime.ExecutionEnvelope{SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "graph-app",
		AgentAppVersion: 2, AgentAppRevision: 3, AgentContentDigest: "graph-denied-digest", ConfigVersion: 4, PolicyVersion: 5,
		RequestID: "graph-denied-request", SessionID: "graph-denied-session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://graph-denied", CreatedAt: time.Now().UTC()}
	rootKey := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: 1, AgentAppID: envelope.AgentAppID,
		AgentAppVersion: 2, AgentAppRevision: 3, ContentDigest: envelope.AgentContentDigest, ConfigVersion: 4, PolicyVersion: 5}
	childKey := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: 1, AgentAppID: "child",
		AgentAppRevision: 1, ContentDigest: childDigest, ConfigVersion: 4, PolicyVersion: 5}
	node := agentapp.AgentNodeSpecV1{Key: "worker", FailurePolicy: agentapp.FailurePolicyFailFast,
		AgentRef: agentapp.PublishedAgentRef{AgentAppID: "child", Revision: 1, ContentDigest: childDigest}}
	root := profile.ExecutionProfileSnapshot{Key: rootKey, TenantVersion: 1, AgentAppVersion: 2,
		ContentDigest: envelope.AgentContentDigest, AgentKind: agentapp.AgentKindGraph,
		AgentSpec: agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{node}, EntryNode: "worker", MaxConcurrency: 1,
			Checkpoint: agentapp.CheckpointPolicyV1{Required: true, Namespace: "workflow"}}}
	child := profile.ExecutionProfileSnapshot{Key: childKey, TenantVersion: 1, ContentDigest: childDigest,
		AgentKind: agentapp.AgentKindLLM, ModelProfileRef: profile.VersionedRef{ID: "model", Version: 9}}
	profiles := profilememory.NewResolver(root, child)
	builds := 0
	bundles := profilememory.NewBundleManager(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		builds++
		return nil, nil, runtime.ErrCapabilityUnsupported
	})
	sessions := sessionmemory.New()
	coordinator := &memoryConfirmationCoordinator{sessions: sessions, confirmation: governance.Confirmation{
		SuspensionRequest: governance.SuspensionRequest{ConfirmationID: "confirmation-denied", TenantID: envelope.TenantID,
			RequestID: envelope.RequestID}, State: governance.ConfirmationDenied, Version: 2}}
	executor := RunnerExecutor{Tasks: taskStub{envelope: envelope}, Profiles: profiles, Bundles: bundles, Sessions: sessions, SDKSessions: agentsessionmemory.NewSessionService(),
		Payloads: messagingmemory.New(), Inputs: JSONTextInputDecoder{}, Confirmations: coordinator, Governance: &confirmationRunGuard{}}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 1, nil); err != nil {
		t.Fatal(err)
	}
	if builds != 0 {
		t.Fatalf("denied Graph acquired a runtime bundle %d times", builds)
	}
	terminal, err := sessions.GetTerminalByInputSeq(context.Background(), sessionstore.TerminalKey{SessionKey: sessionstore.SessionKey{
		TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}, InputSeq: 1})
	if err != nil || terminal.Outcome != runtime.OutcomeConfirmationDenied {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
}
