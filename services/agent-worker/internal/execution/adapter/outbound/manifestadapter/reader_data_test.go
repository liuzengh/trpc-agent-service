package manifestadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gowebpki/jcs"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
)

// This is a Reader unit test with a detached in-memory projection, not a
// publication/compiler, PostgreSQL/NATS, or runtime backend acceptance test.
type dataProjection struct {
	publication manifest.Publication
	reads       int
}

func (p *dataProjection) Apply(context.Context, manifest.Publication, int) error {
	return errors.New("read-only test projection")
}
func (p *dataProjection) Read(_ context.Context, tenant, id string) (manifest.Publication, error) {
	p.reads++
	if tenant != p.publication.TenantID || id != p.publication.ManifestID {
		return manifest.Publication{}, manifest.ErrMissing
	}
	out := p.publication
	out.Envelope = append([]byte(nil), out.Envelope...)
	return out, nil
}
func readerDataFixture(t *testing.T) map[string]any {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	for dir := cwd; ; dir = filepath.Dir(dir) {
		raw, err = os.ReadFile(filepath.Join(dir, "api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json"))
		if err == nil {
			break
		}
		if filepath.Dir(dir) == dir {
			t.Fatal(err)
		}
	}
	var event map[string]any
	if err = json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	return event["manifest"].(map[string]any)
}
func readerDataNode(content map[string]any) map[string]any {
	return content["agent_plan"].(map[string]any)["nodes"].(map[string]any)["assistant"].(map[string]any)
}
func readerDataInput(t *testing.T, change func(map[string]any)) (Reader, domain.Route, []byte, *dataProjection) {
	t.Helper()
	env := readerDataFixture(t)
	content := env["content"].(map[string]any)
	if change != nil {
		change(content)
	}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	env["content_digest"] = digest
	envelope, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	p := &dataProjection{publication: manifest.Publication{TenantID: env["tenant_id"].(string), ManifestID: env["manifest_id"].(string), DeploymentRevisionID: env["deployment_revision_id"].(string), ContentDigest: digest, Envelope: envelope}}
	route := domain.Route{TenantID: p.publication.TenantID, ManifestRef: p.publication.ManifestID, DeploymentRevisionID: p.publication.DeploymentRevisionID, ManifestDigest: digest}
	reader := Reader{Projection: p, ContractDigest: content["platform_contract"].(map[string]any)["digest"].(string)}
	return reader, route, envelope, p
}
func TestReaderResolveLegacyManifestStillBuildsPlan(t *testing.T) {
	reader, route, raw, p := readerDataInput(t, nil)
	if _, err := protocol.DecodeRuntimeManifest(raw); err != nil {
		t.Fatal(err)
	}
	plan, err := reader.Resolve(context.Background(), route)
	if err != nil {
		t.Fatal(err)
	}
	if p.reads != 1 || plan.ManifestID != route.ManifestRef || plan.ManifestDigest != route.ManifestDigest || plan.ModelName != "chat-model" || plan.NodeID != "assistant" || plan.SessionTarget.Username != "session_runtime" {
		t.Fatalf("wrong legacy plan %+v reads=%d", plan, p.reads)
	}
}
func TestReaderResolveWireValidDataCapabilitiesAreUnsupported(t *testing.T) {
	summary := func(c map[string]any) {
		c["runtime"] = map[string]any{"summary": map[string]any{"enabled": true, "model_resource": "primary", "event_threshold": 3}}
	}
	memory := func(c map[string]any) {
		readerDataNode(c)["memory"] = map[string]any{"resource": "memory", "tools": []string{"memory_add", "memory_search"}, "preload_limit": -1}
	}
	artifact := func(c map[string]any) {
		readerDataNode(c)["artifact"] = map[string]any{"enabled": true, "resource": "artifact"}
	}
	consume := func(c map[string]any) { readerDataNode(c)["add_session_summary"] = true }
	cases := []struct {
		name   string
		change func(map[string]any)
	}{{"node memory", memory}, {"node artifact", artifact}, {"node summary consumption", consume}, {"all capabilities", func(c map[string]any) { summary(c); memory(c); artifact(c); consume(c) }}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, route, raw, p := readerDataInput(t, tc.change)
			envelope, err := protocol.DecodeRuntimeManifest(raw)
			if err != nil {
				t.Fatalf("fixture must pass strict full wire decoder: %v", err)
			}
			if _, err = protocol.VerifyRuntimeManifest(envelope); err != nil {
				t.Fatalf("fixture must pass full digest verification: %v", err)
			}
			plan, err := reader.Resolve(context.Background(), route)
			if !errors.Is(err, application.ErrManifestUnsupported) || !reflect.DeepEqual(plan, domain.Plan{}) || p.reads != 1 {
				t.Fatalf("expected Unsupported/empty Plan, got %+v %v reads=%d", plan, err, p.reads)
			}
		})
	}
}
func TestReaderResolveNullAndFalseDataAreInvalid(t *testing.T) {
	for _, field := range []string{"runtime", "summary", "memory", "artifact", "add_session_summary"} {
		for _, value := range []any{nil, false} {
			name := field + " null"
			if value != nil {
				name = field + " false"
			}
			t.Run(name, func(t *testing.T) {
				reader, route, raw, p := readerDataInput(t, func(c map[string]any) {
					switch field {
					case "runtime":
						c[field] = value
					case "summary":
						c["runtime"] = map[string]any{field: value}
					default:
						readerDataNode(c)[field] = value
					}
				})
				if _, err := protocol.DecodeRuntimeManifest(raw); err == nil {
					t.Fatal("strict decoder accepted invalid data")
				}
				plan, err := reader.Resolve(context.Background(), route)
				if !errors.Is(err, application.ErrManifestInvalid) || !reflect.DeepEqual(plan, domain.Plan{}) || p.reads != 1 {
					t.Fatalf("expected Invalid/empty Plan, got %+v %v", plan, err)
				}
			})
		}
	}
}

func TestReaderSummaryBuildsOnlyFixedPublishedPlan(t *testing.T) {
	for _, consume := range []bool{false, true} {
		reader, route, _, projection := readerDataInput(t, func(c map[string]any) {
			c["runtime"] = map[string]any{"summary": map[string]any{"enabled": true, "model_resource": "primary", "event_threshold": 5}}
			if consume {
				readerDataNode(c)["add_session_summary"] = true
			}
		})
		p, err := reader.Resolve(context.Background(), route)
		if err != nil {
			t.Fatal(err)
		}
		if p.Summary == nil || p.Summary.ModelEndpoint != p.ModelEndpoint || p.Summary.ModelName != p.ModelName || p.Summary.ModelCredential != p.ModelCredential || p.Summary.EventThreshold != 5 || p.Summary.AddSessionSummary != consume || projection.reads != 1 {
			t.Fatalf("not a fixed summary projection: %+v", p.Summary)
		}
		if len(p.Uses()) != 2 {
			t.Fatal("shared model credential duplicated")
		}
		// Config is owned by this returned Plan, not shared between resolutions.
		p.Summary.ModelName = "mutated"
		next, err := reader.Resolve(context.Background(), route)
		if err != nil || next.Summary.ModelName == "mutated" {
			t.Fatal("summary aliases another Plan", err)
		}
	}
}
