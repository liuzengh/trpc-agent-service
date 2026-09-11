// These tests close WV-05 at the actual broker/PG/Reader/Processor/SDK seam.
// Profile and model HTTP are explicit fixtures, not real Control publication.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5/pgxpool"
	controlevents "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	executionwire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/manifestadapter"
	ledgerpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/runtimeadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/natsadapter"
	projectionpg "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

type manifestGateLedger struct {
	application.Ledger
	claims atomic.Int32
}

func (l *manifestGateLedger) Claim(ctx context.Context, req domain.ClaimRequest) (domain.Grant, error) {
	l.claims.Add(1)
	return l.Ledger.Claim(ctx, req)
}

type manifestGateRuntime struct {
	application.RuntimeFactory
	prepares, executes atomic.Int32
}

func (r *manifestGateRuntime) Prepare(ctx context.Context, g domain.Grant, p domain.Plan, check func(context.Context) error) (application.AttemptRuntime, error) {
	r.prepares.Add(1)
	a, e := r.RuntimeFactory.Prepare(ctx, g, p, check)
	if e != nil {
		return nil, e
	}
	return &manifestGateAttempt{AttemptRuntime: a, calls: &r.executes}, nil
}

type manifestGateAttempt struct {
	application.AttemptRuntime
	calls *atomic.Int32
}

func (r *manifestGateAttempt) Execute(ctx context.Context, b []byte) (domain.RuntimeResult, error) {
	r.calls.Add(1)
	return r.AttemptRuntime.Execute(ctx, b)
}

type manifestGateReader struct {
	manifestadapter.Reader
	result string
}

func (r *manifestGateReader) Resolve(ctx context.Context, route domain.Route) (domain.Plan, error) {
	p, e := r.Reader.Resolve(ctx, route)
	switch {
	case e == nil:
		r.result = "VALID"
	case errors.Is(e, application.ErrManifestMissing):
		r.result = "MISSING"
	case errors.Is(e, application.ErrManifestInvalid):
		r.result = "INVALID"
	case errors.Is(e, application.ErrManifestUnsupported):
		r.result = "UNSUPPORTED"
	case errors.Is(e, application.ErrManifestContractMismatch):
		r.result = "CONTRACT_MISMATCH"
	default:
		r.result = "UNEXPECTED_DEPENDENCY"
	}
	return p, e
}

type manifestGateFixture struct {
	ctx                  context.Context
	admin                *pgxpool.Pool
	ledger               *ledgerpg.Ledger
	observed             *manifestGateLedger
	runtime              *manifestGateRuntime
	reader               manifestadapter.Reader
	js                   jetstream.JetStream
	runs, manifests      *natsadapter.Consumer
	base                 controlevents.RuntimeManifestPublishedEvent
	modelCalls, resolves *atomic.Int32
	root                 string
}

func setupManifestGate(t *testing.T) *manifestGateFixture {
	t.Helper()
	keys := []string{"WORKER_TEST_ADMIN_URL", "WORKER_TEST_MIGRATION_URL", "WORKER_TEST_RUNTIME_URL", "WORKER_SESSION_TEST_MIGRATION_URL", "WORKER_SESSION_TEST_RUNTIME_URL", "WORKER_TEST_NATS_URL"}
	for _, key := range keys {
		if os.Getenv(key) == "" {
			t.Skip("requires dedicated Worker vertical fixtures")
		}
	}
	if os.Getenv("WORKER_TEST_ALLOW_RESET") != "1" {
		t.Fatal("explicit fixture reset required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	open := func(key string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, os.Getenv(key))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	admin, migration, runtime, sessionMigration := open(keys[0]), open(keys[1]), open(keys[2]), open(keys[3])
	if err := migrations.ApplyForRuntime(ctx, migration, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	if err := sessionmigrations.Apply(ctx, sessionMigration); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `TRUNCATE worker.execution_sessions CASCADE;TRUNCATE worker.execution_rejections;TRUNCATE worker.runtime_manifests CASCADE;TRUNCATE worker.manifest_conflicts;TRUNCATE worker.manifest_conflict_identities;TRUNCATE runtime_session.session_candidates`); err != nil {
		t.Fatal(err)
	}
	conn, err := nats.Connect(os.Getenv(keys[5]), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		name, subject, durable string
		retention              jetstream.RetentionPolicy
	}{
		{natsadapter.RunStream, natsadapter.RunSubject, natsadapter.RunDurable, jetstream.WorkQueuePolicy},
		{natsadapter.ManifestStream, natsadapter.ManifestSubject, natsadapter.ManifestDurable, jetstream.LimitsPolicy},
		{natsadapter.ReplyStream, natsadapter.ReplySubject, "channel-gateway-replies-v1", jetstream.WorkQueuePolicy},
	} {
		if _, err = js.Stream(ctx, item.name); err == nil {
			t.Fatalf("dedicated broker already has %s", item.name)
		}
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: item.name, Subjects: []string{item.subject}, Retention: item.retention, Storage: jetstream.FileStorage, Discard: jetstream.DiscardNew, MaxBytes: 16 << 20, MaxMsgSize: 1 << 20, DenyDelete: true, DenyPurge: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, cc := context.WithTimeout(context.Background(), 5*time.Second)
			defer cc()
			nc, e := nats.Connect(os.Getenv(keys[5]), nats.NoReconnect())
			if e == nil {
				defer nc.Close()
				j, e := jetstream.New(nc)
				if e == nil {
					_ = j.DeleteStream(cleanup, item.name)
				}
			}
		})
		if _, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: item.durable, FilterSubject: item.subject, AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverAllPolicy, ReplayPolicy: jetstream.ReplayInstantPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 64}); err != nil {
			t.Fatal(err)
		}
	}
	var modelCalls atomic.Int32
	var requestsMu sync.Mutex
	var requests []map[string]any
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callNumber := modelCalls.Add(1)
		var req map[string]any
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		requestsMu.Lock()
		requests = append(requests, req)
		requestsMu.Unlock()
		if r.Header.Get("Authorization") != "Bearer fixture-model-key" {
			http.Error(w, "bad key", 403)
			return
		}
		answer := "First answer"
		if callNumber > 1 {
			answer = "Second answer"
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":%q},\"finish_reason\":null}]}\n\n", answer)
		fmt.Fprint(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(model.Close)
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		next := filepath.Dir(dir)
		if next == dir {
			t.Fatal("root missing")
		}
		dir = next
	}
	raw, err := os.ReadFile(filepath.Join(dir, "api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := controlevents.DecodeRuntimeManifestPublishedEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	content, err := protocol.VerifyRuntimeManifest(event.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(os.Getenv(keys[4]))
	if err != nil {
		t.Fatal(err)
	}
	modelResource := content.Resources.Models["primary"]
	modelResource.BaseURL = model.URL + "/v1"
	modelResource.Credential.AudienceDigest = protocol.CredentialAudienceDigest(modelResource.Kind, modelResource.BaseURL)
	content.Resources.Models["primary"] = modelResource
	storage := content.Resources.Storage["session"]
	storage.Destination = protocol.StorageDestination{Host: config.ConnConfig.Host, Port: int64(config.ConnConfig.Port), Database: config.ConnConfig.Database, Username: config.ConnConfig.User, SSLMode: "disable"}
	storage.Credential.AudienceDigest = protocol.CredentialAudienceDigest(storage.Kind, storage.Destination)
	content.Resources.Storage["session"] = storage
	content.Execution.AllowedEndpointHosts = []string{"127.0.0.1"}
	content.Execution.MaxOutputTokens = 20000
	contentBytes, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	contentBytes, err = jcs.Transform(contentBytes)
	if err != nil {
		t.Fatal(err)
	}
	event.Manifest.Content = contentBytes
	event.Manifest.ContentDigest = domain.Digest(contentBytes)
	event.OccurredAt = time.Now().UTC()
	event.Manifest.PublishedAt = event.OccurredAt
	manifestWire, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = controlevents.DecodeRuntimeManifestPublishedEvent(manifestWire); err != nil {
		t.Fatal(err)
	}
	ledger := ledgerpg.New(runtime)
	projection := projectionpg.New(runtime)
	reader := manifestadapter.Reader{Projection: projection, ContractDigest: content.PlatformContract.Digest}
	var resolves atomic.Int32
	profile := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resolves.Add(1)
		var input struct {
			ExecutionToken string           `json:"execution_token"`
			ManifestID     string           `json:"manifest_id"`
			ManifestDigest string           `json:"manifest_digest"`
			Uses           []map[string]any `json:"uses"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		grant, e := ledger.ActiveToken(r.Context(), "worker-fixture", input.ExecutionToken, input.ManifestID, input.ManifestDigest)
		if e != nil {
			http.Error(w, "denied", 403)
			return
		}
		credentials := []map[string]any{}
		for _, u := range []protocol.CredentialUse{modelResource.Credential, storage.Credential} {
			value := "fixture-model-key"
			if u.Purpose == "dsn" {
				value = config.ConnConfig.Password
			}
			credentials = append(credentials, map[string]any{"credential_id": u.CredentialID, "purpose": u.Purpose, "audience_digest": u.AudienceDigest, "credential_revision": 1, "value": value})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": grant.Run.Request.Route.TenantID, "profile_id": content.Sources.Profile.ProfileID, "profile_revision_number": content.Sources.Profile.RevisionNumber, "run_id": grant.Run.Request.RunID, "attempt_id": grant.AttemptID, "worker_id": grant.WorkerID, "lease_epoch": grant.LeaseEpoch, "manifest_id": input.ManifestID, "manifest_digest": input.ManifestDigest, "credentials": credentials})
	}))
	t.Cleanup(profile.Close)
	factory, err := runtimeadapter.New(runtimeadapter.Options{BaseURL: profile.URL, Client: profile.Client(), RequestTimeout: 3 * time.Second, MaxResponseBytes: 1 << 20, SnapshotCapacityBytes: 1 << 20, DrainTimeout: time.Second, MaxTrackedAttempts: 100})
	if err != nil {
		t.Fatal(err)
	}
	policy := domain.Policy{Version: "integration-v1", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 5 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Millisecond, MaxAttempts: 3}
	acceptor, err := application.NewAcceptor(ledger, policy, domain.IntakeLimits{MaxQueuedRuns: 100, MaxRetainedRuns: 100000})
	if err != nil {
		t.Fatal(err)
	}

	runs, err := natsadapter.BindRun(ctx, js, acceptor, ledger)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := natsadapter.BindManifest(ctx, js, projection, ledger, 100)
	if err != nil {
		t.Fatal(err)
	}
	observed := &manifestGateLedger{Ledger: ledger}
	execution := &manifestGateRuntime{RuntimeFactory: factory}
	return &manifestGateFixture{ctx: ctx, admin: admin, ledger: ledger, observed: observed,
		runtime: execution, reader: reader, js: js, runs: runs, manifests: manifests,
		base: event, modelCalls: &modelCalls, resolves: &resolves, root: dir}
}

type manifestGateCase struct {
	name, reader, reason  string
	invalidWire, conflict bool
}

var manifestGateCases = []manifestGateCase{
	{name: "event_revision_mismatch", reader: "MISSING", invalidWire: true},
	{name: "content_digest_mismatch", reader: "MISSING", invalidWire: true},
	{name: "schema_version_mismatch", reader: "MISSING", invalidWire: true},
	{name: "legacy_platform_version", reader: "UNSUPPORTED", reason: "UNSUPPORTED_MANIFEST"},
	{name: "release_pin_mismatch", reader: "CONTRACT_MISMATCH"},
	{name: "fixed_manifest_reference_mismatch", reader: "MISSING"},
	{name: "fixed_revision_mismatch", reader: "INVALID", reason: "MANIFEST_INVALID"},
	{name: "fixed_digest_mismatch", reader: "INVALID", reason: "MANIFEST_INVALID"},
	{name: "cross_tenant_run", reader: "MISSING"},
	{name: "public_view_as_envelope", reader: "MISSING", invalidWire: true},
	{name: "public_view_as_content", reader: "MISSING", invalidWire: true},
	{name: "conflicting_manifest_identity", reader: "INVALID", reason: "MANIFEST_INVALID", conflict: true},
}

func manifestGateJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
func manifestGateEvent(t *testing.T, base controlevents.RuntimeManifestPublishedEvent, index int) controlevents.RuntimeManifestPublishedEvent {
	t.Helper()
	var event controlevents.RuntimeManifestPublishedEvent
	if e := json.Unmarshal(manifestGateJSON(t, base), &event); e != nil {
		t.Fatal(e)
	}
	event.EventID = fmt.Sprintf("evt_manifest_gate_%d", index)
	event.DeploymentRevisionID = fmt.Sprintf("revision_manifest_gate_%d", index)
	event.Manifest.DeploymentRevisionID = event.DeploymentRevisionID
	event.Manifest.ID = fmt.Sprintf("manifest_gate_%d", index)
	return event
}
func manifestGateRecontent(t *testing.T, event *controlevents.RuntimeManifestPublishedEvent, change func(*protocol.ManifestContent)) {
	t.Helper()
	c, e := protocol.VerifyRuntimeManifest(event.Manifest)
	if e != nil {
		t.Fatal(e)
	}
	change(&c)
	raw, e := jcs.Transform(manifestGateJSON(t, c))
	if e != nil {
		t.Fatal(e)
	}
	event.Manifest.Content = raw
	event.Manifest.ContentDigest = domain.Digest(raw)
}
func manifestGateVariant(t *testing.T, event controlevents.RuntimeManifestPublishedEvent, name string, view []byte) []byte {
	t.Helper()
	switch name {
	case "event_revision_mismatch":
		event.Manifest.DeploymentRevisionID += "_mismatch"
	case "content_digest_mismatch":
		event.Manifest.ContentDigest = domain.Digest([]byte("wrong digest"))
	case "schema_version_mismatch":
		manifestGateRecontent(t, &event, func(c *protocol.ManifestContent) { c.SchemaVersion = "v2" })
	case "legacy_platform_version":
		manifestGateRecontent(t, &event, func(c *protocol.ManifestContent) { c.PlatformContract.Version = "platform-v1" })
	case "conflicting_manifest_identity":
		event.EventID += "_conflict"
		manifestGateRecontent(t, &event, func(c *protocol.ManifestContent) {
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			n.Instruction = "Different immutable instruction"
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
		})
	case "public_view_as_envelope":
		var object map[string]json.RawMessage
		if e := json.Unmarshal(manifestGateJSON(t, event), &object); e != nil {
			t.Fatal(e)
		}
		object["manifest"] = json.RawMessage(view)
		return manifestGateJSON(t, object)
	case "public_view_as_content":
		raw, e := jcs.Transform(view)
		if e != nil {
			t.Fatal(e)
		}
		event.Manifest.Content = raw
		event.Manifest.ContentDigest = domain.Digest(raw)
	}
	return manifestGateJSON(t, event)
}
func manifestGateRoot(t *testing.T) string {
	t.Helper()
	dir, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	for {
		if _, e = os.Stat(filepath.Join(dir, "go.mod")); e == nil {
			return dir
		}
		next := filepath.Dir(dir)
		if next == dir {
			t.Fatal("repository root missing")
		}
		dir = next
	}
}
func manifestGatePublicView(t *testing.T, root string) []byte {
	t.Helper()
	raw, e := os.ReadFile(filepath.Join(root, "api/schemas/deployment/v1/examples/valid/runtime-manifest-view-worker-v1.json"))
	if e != nil {
		t.Fatal(e)
	}
	// A real versioned public View fixture must pass its own public schema before
	// it is used as the negative executable input. No arbitrary truncated JSON.
	var document any
	if e = json.Unmarshal(protocol.ManifestViewSchema, &document); e != nil {
		t.Fatal(e)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	const location = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest-view.schema.json"
	if e = compiler.AddResource(location, document); e != nil {
		t.Fatal(e)
	}
	schema, e := compiler.Compile(location)
	if e != nil {
		t.Fatal(e)
	}
	var value any
	if e = json.Unmarshal(raw, &value); e != nil {
		t.Fatal(e)
	}
	if e = schema.Validate(value); e != nil {
		t.Fatal("public View fixture is invalid", e)
	}
	return raw
}

// This dependency-free test only checks that each test input selects the stated
// codec/contract layer. It is NOT the zero-Processor/SDK integration evidence.
func TestWorkerManifestGateFixtureContracts(t *testing.T) {
	root := manifestGateRoot(t)
	raw, e := os.ReadFile(filepath.Join(root, "api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json"))
	if e != nil {
		t.Fatal(e)
	}
	base, e := controlevents.DecodeRuntimeManifestPublishedEvent(raw)
	if e != nil {
		t.Fatal(e)
	}
	view := manifestGatePublicView(t, root)
	for index, tc := range manifestGateCases {
		t.Run(tc.name, func(t *testing.T) {
			event := manifestGateEvent(t, base, index)
			wire := manifestGateVariant(t, event, tc.name, view)
			decoded, e := controlevents.DecodeRuntimeManifestPublishedEvent(wire)
			if tc.invalidWire {
				if !errors.Is(e, controlevents.ErrInvalidManifestEvent) {
					t.Fatal("negative did not reach strict decoder", e)
				}
				return
			}
			if e != nil {
				t.Fatal("expected complete valid wire, not schema rejection", e)
			}
			c, e := protocol.VerifyRuntimeManifest(decoded.Manifest)
			if e != nil {
				t.Fatal(e)
			}
			expected := c.PlatformContract.Digest
			if tc.name == "release_pin_mismatch" {
				expected = domain.Digest([]byte("different release pin"))
			}
			e = protocol.ValidateWorkerV1(c, expected)
			if tc.reader == "UNSUPPORTED" {
				if !errors.Is(e, protocol.ErrUnsupportedWorkerManifest) {
					t.Fatal("expected capability gate refusal", e)
				}
			} else if tc.reader == "CONTRACT_MISMATCH" {
				if !errors.Is(e, protocol.ErrWorkerV1ContractMismatch) {
					t.Fatal("expected release-pin skew, not a capability gate refusal", e)
				}
			} else if e != nil {
				t.Fatal("unrelated policy failure masks tested layer", e)
			}
		})
	}
}
func (f *manifestGateFixture) counts() [5]int32 {
	return [5]int32{f.observed.claims.Load(), f.runtime.prepares.Load(), f.runtime.executes.Load(), f.resolves.Load(), f.modelCalls.Load()}
}
func (f *manifestGateFixture) scalar(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if e := f.admin.QueryRow(f.ctx, query, args...).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func (f *manifestGateFixture) publish(t *testing.T, raw []byte) (uint64, string) {
	t.Helper()
	ack, e := f.js.Publish(f.ctx, natsadapter.ManifestSubject, raw)
	if e != nil {
		t.Fatal(e)
	}
	got, e := f.manifests.Poll(f.ctx)
	if e != nil || !got {
		t.Fatalf("real Manifest poll: found=%v err=%v", got, e)
	}
	stream, e := f.js.Stream(f.ctx, natsadapter.ManifestStream)
	if e != nil {
		t.Fatal(e)
	}
	info, e := stream.Info(f.ctx)
	if e != nil {
		t.Fatal(e)
	}
	consumer, e := stream.Consumer(f.ctx, natsadapter.ManifestDurable)
	if e != nil {
		t.Fatal(e)
	}
	ci, e := consumer.Info(f.ctx)
	if e != nil {
		t.Fatal(e)
	}
	if ci.AckFloor.Stream < ack.Sequence || ci.NumAckPending != 0 {
		t.Fatal("Manifest owner fact not followed by durable broker ACK", ci.AckFloor, ci.NumAckPending)
	}
	return ack.Sequence, fmt.Sprintf("%s@%s#%d", natsadapter.ManifestStream, info.Created.UTC().Format(time.RFC3339Nano), ack.Sequence)
}
func (f *manifestGateFixture) intake(t *testing.T, event controlevents.RuntimeManifestPublishedEvent, index int, name string) domain.Run {
	t.Helper()
	req := dto.RunRequested{SchemaVersion: 1, EventID: fmt.Sprintf("evt_manifest_gate_run_%d", index), AdmissionID: fmt.Sprintf("adm_manifest_gate_%d", index), RunID: fmt.Sprintf("run_manifest_gate_%d", index), Route: dto.RouteSnapshot{Provider: "telegram", AccountID: "account_fixture", Generation: 1, TenantID: event.TenantID, BindingID: "binding_fixture", DeploymentRevisionID: event.DeploymentRevisionID, ManifestRef: event.Manifest.ID, ManifestDigest: event.Manifest.ContentDigest}, Input: dto.Inbound{Key: dto.EventKey{Provider: "telegram", AccountID: "account_fixture", EventID: fmt.Sprint(200 + index)}, Kind: "text", ConversationID: fmt.Sprint(1000 + index), SenderID: "43", Text: "Manifest gate " + name, ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano), ReplyContext: &dto.ReplyContext{ChatID: fmt.Sprint(1000 + index)}, SourceDigest: strings.Repeat("a", 64)}}
	switch name {
	case "fixed_manifest_reference_mismatch":
		req.Route.ManifestRef += "_absent"
	case "fixed_revision_mismatch":
		req.Route.DeploymentRevisionID += "_wrong"
	case "fixed_digest_mismatch":
		req.Route.ManifestDigest = domain.Digest([]byte("wrong fixed route digest"))
	case "cross_tenant_run":
		req.Route.TenantID = "other-tenant"
	}
	raw, e := executionwire.EncodeRunRequested(req)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.js.Publish(f.ctx, natsadapter.RunSubject, raw); e != nil {
		t.Fatal(e)
	}
	if got, e := f.runs.Poll(f.ctx); e != nil || !got {
		t.Fatalf("real Run intake found=%v err=%v", got, e)
	}
	run, e := f.ledger.FindRun(f.ctx, req.Route.TenantID, req.RunID)
	if e != nil {
		t.Fatal(e)
	}
	if run.Sequence != 1 || run.Status != domain.Queued || run.WaitReason != "MANIFEST" {
		t.Fatal("Run not durably waiting before Processor", run.Status, run.Sequence, run.WaitReason)
	}
	if f.scalar(t, `SELECT count(*) FROM worker.execution_receipts WHERE event_id=$1 AND outcome='ACCEPTED'`, req.EventID) != 1 {
		t.Fatal("no durable Run intake receipt")
	}
	return run
}

func TestWorkerManifestRejectionPGNATSSDK(t *testing.T) {
	f := setupManifestGate(t)
	view := manifestGatePublicView(t, f.root)
	checks := append([]manifestGateCase{{name: "valid_before", reader: "VALID"}}, manifestGateCases...)
	checks = append(checks, manifestGateCase{name: "valid_after", reader: "VALID"})
	for index, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			before := f.counts()
			event := manifestGateEvent(t, f.base, index)
			raw := manifestGateVariant(t, event, tc.name, view)
			decoded, decodeErr := controlevents.DecodeRuntimeManifestPublishedEvent(raw)
			if tc.invalidWire {
				if !errors.Is(decodeErr, controlevents.ErrInvalidManifestEvent) {
					t.Fatal("test input is not the intended invalid wire", decodeErr)
				}
			} else if decodeErr != nil {
				t.Fatal("test input accidentally hit codec rather than target branch", decodeErr)
			}
			routeEvent := event
			if decodeErr == nil && !tc.conflict {
				routeEvent = decoded
			}
			if tc.conflict {
				f.publish(t, manifestGateJSON(t, event))
				if f.scalar(t, `SELECT count(*) FROM worker.runtime_manifests WHERE manifest_id=$1 AND content_digest=$2 AND NOT conflicted`, event.Manifest.ID, event.Manifest.ContentDigest) != 1 {
					t.Fatal("valid first immutable projection missing")
				}
			}
			sequence, source := f.publish(t, raw)
			wireRejections := f.scalar(t, `SELECT count(*) FROM worker.execution_rejections WHERE source_identity=$1 AND digest=$2 AND reason='INVALID_MANIFEST_WIRE'`, source, domain.Digest(raw))
			projectionRows := f.scalar(t, `SELECT count(*) FROM worker.runtime_manifests WHERE manifest_id=$1`, event.Manifest.ID)
			var consumerResult string
			switch {
			case tc.invalidWire:
				consumerResult = "DURABLE_INVALID_MANIFEST_WIRE"
				if wireRejections != 1 || projectionRows != 0 || f.scalar(t, `SELECT count(*) FROM worker.manifest_receipts WHERE event_id=$1`, event.EventID) != 0 {
					t.Fatal("invalid wire was projected or not durably rejected")
				}
			case tc.conflict:
				consumerResult = "DURABLE_IDENTITY_CONFLICT"
				if wireRejections != 0 || projectionRows != 1 {
					t.Fatal("wrong conflict projection result")
				}
				if f.scalar(t, `SELECT count(*) FROM worker.manifest_conflicts WHERE event_id=$1 AND reason='IDENTITY_CONFLICT'`, decoded.EventID) != 1 {
					t.Fatal("conflict not durable")
				}
				if f.scalar(t, `SELECT count(*) FROM worker.runtime_manifests WHERE manifest_id=$1 AND content_digest=$2 AND conflicted`, event.Manifest.ID, event.Manifest.ContentDigest) != 1 {
					t.Fatal("conflict replaced original content or failed to isolate it")
				}
			default:
				consumerResult = "DURABLE_COMPLETE_PROJECTION"
				if wireRejections != 0 || projectionRows != 1 || f.scalar(t, `SELECT count(*) FROM worker.manifest_receipts WHERE event_id=$1`, event.EventID) != 1 {
					t.Fatal("complete manifest did not reach projection")
				}
			}
			run := f.intake(t, routeEvent, index, tc.name)
			reader := &manifestGateReader{Reader: f.reader}
			if tc.name == "release_pin_mismatch" {
				reader.ContractDigest = domain.Digest([]byte("different release pin"))
			}
			processor, e := application.NewProcessor(f.observed, reader, f.runtime, "worker-fixture", 4)
			if e != nil {
				t.Fatal(e)
			}
			advanceErr := processor.Advance(f.ctx, run)
			if reader.result != tc.reader {
				t.Fatalf("actual Processor reader=%s want=%s", reader.result, tc.reader)
			}
			switch tc.reader {
			case "MISSING":
				if !errors.Is(advanceErr, application.ErrManifestMissing) {
					t.Fatal(advanceErr)
				}
			case "CONTRACT_MISMATCH":
				if !errors.Is(advanceErr, application.ErrManifestContractMismatch) {
					t.Fatal(advanceErr)
				}
			default:
				if advanceErr != nil {
					t.Fatal(advanceErr)
				}
			}
			finalRun, e := f.ledger.FindRun(f.ctx, run.Request.Route.TenantID, run.Request.RunID)
			if e != nil {
				t.Fatal(e)
			}
			delta := f.counts()
			for i := range delta {
				delta[i] -= before[i]
			}
			attempts := f.scalar(t, `SELECT count(*) FROM worker.execution_attempts WHERE tenant_id=$1 AND run_id=$2`, run.Request.Route.TenantID, run.Request.RunID)
			candidates := f.scalar(t, `SELECT count(*) FROM runtime_session.session_candidates WHERE tenant_id=$1 AND run_id=$2`, run.Request.Route.TenantID, run.Request.RunID)
			finals := f.scalar(t, `SELECT count(*) FROM worker.execution_reply_outbox WHERE tenant_id=$1 AND run_id=$2`, run.Request.Route.TenantID, run.Request.RunID)
			var acceptedRef, acceptedDigest string
			var settled int64
			if e = f.admin.QueryRow(f.ctx, `SELECT accepted_ref,accepted_digest,settled_sequence FROM worker.execution_sessions WHERE tenant_id=$1 AND session_id=$2`, run.Request.Route.TenantID, run.SessionID).Scan(&acceptedRef, &acceptedDigest, &settled); e != nil {
				t.Fatal(e)
			}
			processorResult := ""
			switch tc.reader {
			case "VALID":
				processorResult = "SDK_SESSION_COMPLETION_FINAL"
				if delta != [5]int32{1, 1, 1, 1, 1} || attempts != 1 || candidates != 1 || finals != 1 || finalRun.Status != domain.Succeeded {
					t.Fatal("valid control did not exercise the real full runtime", delta, attempts, candidates, finals, finalRun.Status)
				}
				completion, e := f.ledger.FindCompletion(f.ctx, run.Request.Route.TenantID, run.Request.RunID)
				if e != nil || completion.Status != domain.Succeeded || completion.Candidate.Ref != acceptedRef || completion.Candidate.Digest != acceptedDigest || acceptedRef == "" || settled != 1 || completion.FinalIntentID == "" {
					t.Fatal("valid control formal Session/Completion mismatch", e)
				}
				if f.scalar(t, `SELECT count(*) FROM worker.execution_session_commits WHERE tenant_id=$1 AND run_id=$2 AND candidate_ref=$3 AND candidate_digest=$4`, run.Request.Route.TenantID, run.Request.RunID, acceptedRef, acceptedDigest) != 1 {
					t.Fatal("valid control Session commit missing")
				}
				proof, e := f.ledger.Final(f.ctx, completion.FinalIntentID)
				if e != nil || proof.RunID != run.Request.RunID || proof.ManifestDigest != run.Request.Route.ManifestDigest {
					t.Fatal("valid control immutable Final proof missing", e)
				}
			default:
				if delta != [5]int32{} || attempts != 0 || candidates != 0 || finals != 0 || acceptedRef != "" || acceptedDigest != "" {
					t.Fatal("negative crossed Claim/Resolve/SDK or accepted state", delta, attempts, candidates, finals)
				}
				if tc.reader == "MISSING" || tc.reader == "CONTRACT_MISMATCH" {
					processorResult = "PERSISTENT_MANIFEST_WAIT_NOT_RUN_REJECTION"
					if tc.reader == "CONTRACT_MISMATCH" {
						processorResult = "RELEASE_SKEW_WAIT_NOT_RUN_REJECTION"
					}
					if finalRun.Status != domain.Queued || finalRun.WaitReason != "MANIFEST" || settled != 0 {
						t.Fatal("unexecutable fixed manifest must remain durable wait", finalRun.Status, finalRun.WaitReason, settled)
					}
					if _, e = f.ledger.FindCompletion(f.ctx, run.Request.Route.TenantID, run.Request.RunID); !errors.Is(e, domain.ErrNotFound) {
						t.Fatal("waiting Run was falsely terminalized", e)
					}
				} else {
					processorResult = "PRECLAIM_SYSTEM_TERMINATION"
					completion, e := f.ledger.FindCompletion(f.ctx, run.Request.Route.TenantID, run.Request.RunID)
					if e != nil || finalRun.Status != domain.Failed || completion.Kind != "SYSTEM_TERMINATION" || completion.Reason != tc.reason || completion.ReplyDisposition != "NONE" || completion.AttemptID != "" || settled != 1 {
						t.Fatalf("negative not terminalized before Claim: %+v %v", completion, e)
					}
				}
			}
			evidence := map[string]any{"case": tc.name, "result": "PASS", "consumer": consumerResult, "reader": reader.result, "processor": processorResult, "manifest_broker_sequence": sequence, "run_id": run.Request.RunID, "run_status": finalRun.Status, "run_sequence": finalRun.Sequence, "claim_calls": delta[0], "prepare_calls": delta[1], "sdk_execute_calls": delta[2], "resolve_http_calls": delta[3], "model_http_calls": delta[4], "durable_attempts": attempts, "durable_candidates": candidates, "final_outbox": finals, "invalid_manifest_receipts": wireRejections, "projection_rows": projectionRows}
			t.Log("WORKER_MANIFEST_REJECTION_CASE=" + string(manifestGateJSON(t, evidence)))
		})
	}
	if t.Failed() {
		return
	}
	if f.counts() != [5]int32{2, 2, 2, 2, 2} {
		t.Fatal("only both valid controls may execute", f.counts())
	}
	t.Logf("WORKER_MANIFEST_REJECTION=PASS negative_cases=%d valid_controls=2; actual PG/NATS strict consumer + durable rejection/conflict/projection + real Reader/Processor; each negative zero Claim/Prepare/Resolve/SDK/model/candidate/Final; missing Manifest remains waiting, not falsely Run-rejected; both valid controls use actual SDK and formal PG Session", len(manifestGateCases))
}
