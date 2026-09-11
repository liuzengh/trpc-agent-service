package bootstrap

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5/pgxpool"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/finalartifacthttp"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type replyPublicationFixture struct {
	value      manifest.Publication
	err        error
	tenant, id string
}

func (s *replyPublicationFixture) Read(_ context.Context, tenant, id string) (manifest.Publication, error) {
	s.tenant = tenant
	s.id = id
	return s.value, s.err
}

type finalCredentialClientFixture struct {
	calls   int
	request finalartifacthttp.Request
	uses    []domain.CredentialUse
}

func (s *finalCredentialClientFixture) Resolve(_ context.Context, r finalartifacthttp.Request, uses []domain.CredentialUse) (artifactstore.Credentials, error) {
	s.calls++
	s.request = r
	s.uses = uses
	return artifactstore.Credentials{AccessKeyID: "access-value", SecretAccessKey: "secret-value"}, nil
}
func replyCredentialFixture(t *testing.T) (proof.ReplyArtifactRequest, domain.AcceptedArtifact, domain.Plan, *replyPublicationFixture) {
	t.Helper()
	_, r, l, m, _, _ := replyQueryFixture(t)
	raw, e := os.ReadFile("../../../../api/schemas/deployment/v1/examples/valid/runtime-manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	envelope, e := protocol.DecodeRuntimeManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	content, e := protocol.VerifyRuntimeManifest(envelope)
	if e != nil {
		t.Fatal(e)
	}
	p := m.plan
	p.TenantID = envelope.TenantID
	p.ManifestID = envelope.ID
	p.DeploymentRevisionID = envelope.DeploymentRevisionID
	p.ProfileID = content.Sources.Profile.ProfileID
	p.ProfileRevision = content.Sources.Profile.RevisionNumber
	p.Artifact.Backend.TenantID = p.TenantID
	digest, e := p.Artifact.Backend.Digest()
	if e != nil {
		t.Fatal(e)
	}
	p.Artifact.AccessKeyID = domain.CredentialUse{CredentialID: "crd_00000000000000000000000000000011", Purpose: "access_key_id", AudienceDigest: digest}
	p.Artifact.SecretAccessKey = domain.CredentialUse{CredentialID: "crd_00000000000000000000000000000012", Purpose: "secret_access_key", AudienceDigest: digest}
	use := func(u domain.CredentialUse) protocol.CredentialUse {
		return protocol.CredentialUse{CredentialID: u.CredentialID, Purpose: u.Purpose, AudienceDigest: u.AudienceDigest}
	}
	content.StorageRoles["artifact"] = "artifact"
	content.Resources.Storage["artifact"] = protocol.ManifestStorageResource{Kind: "managed_artifact", AdapterVersion: "managed-artifact-v1", Backend: &p.Artifact.Backend, MetadataContract: protocol.ArtifactMetadataContract, Credentials: &protocol.ArtifactCredentials{AccessKeyID: use(p.Artifact.AccessKeyID), SecretAccessKey: use(p.Artifact.SecretAccessKey)}}
	n := content.AgentPlan.Nodes[content.AgentPlan.Root]
	n.Artifact = &protocol.ManifestArtifact{Enabled: true, Resource: "artifact"}
	content.AgentPlan.Nodes[content.AgentPlan.Root] = n
	body, e := json.Marshal(content)
	if e != nil {
		t.Fatal(e)
	}
	canonical, e := jcs.Transform(body)
	if e != nil {
		t.Fatal(e)
	}
	envelope.Content = canonical
	envelope.ContentDigest = domain.Digest(canonical)
	p.ManifestDigest = envelope.ContentDigest
	encoded, e := json.Marshal(envelope)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = protocol.DecodeRuntimeManifest(encoded); e != nil {
		t.Fatal("complete internal manifest fixture", e)
	}
	accepted := l.value
	accepted.Run.Request.Route = domain.Route{TenantID: p.TenantID, ManifestRef: p.ManifestID, ManifestDigest: p.ManifestDigest, DeploymentRevisionID: p.DeploymentRevisionID}
	accepted.Run.Request.AdmissionID = "admission"
	accepted.Run.CurrentAttemptID = "attempt"
	accepted.Run.Generation = 2
	accepted.Final = domain.Final{TenantID: p.TenantID, IntentID: r.IntentID, Digest: domain.Digest([]byte("committed final")), AdmissionID: "admission", RunID: r.RunID, AttemptID: "attempt", CompletionID: r.CompletionID, ManifestDigest: p.ManifestDigest, ExecutionGeneration: 2, Sequence: 1}
	publication := &replyPublicationFixture{value: manifest.Publication{TenantID: p.TenantID, ManifestID: p.ManifestID, DeploymentRevisionID: p.DeploymentRevisionID, ContentDigest: p.ManifestDigest, EnvelopeDigest: domain.Digest(encoded), Envelope: encoded}}
	return r, accepted, p, publication
}
func TestReplyArtifactCredentialRequestUsesDurableFinalAndPublishedEnvelope(t *testing.T) {
	r, a, p, projection := replyCredentialFixture(t)
	client := &finalCredentialClientFixture{}
	q := replyArtifactCredentialResolver{projection: projection, client: client}
	out, e := q.ResolveReplyArtifact(context.Background(), r, a, p)
	if e != nil || out.AccessKeyID != "access-value" || client.calls != 1 || projection.tenant != a.Run.Request.Route.TenantID || projection.id != a.Run.Request.Route.ManifestRef {
		t.Fatal(e, client.calls)
	}
	if client.request.DeploymentID != "dpl_fixture_deployment" || client.request.Final.Digest != a.Final.Digest || client.request.Final.ExecutionGeneration != 2 || client.request.ProfileID != p.ProfileID || client.request.ProfileRevisionNumber != p.ProfileRevision || len(client.uses) != 2 {
		t.Fatal("fixed publication not preserved", client.request)
	}
}
func TestReplyArtifactCredentialIdentityMismatchNeverCallsControl(t *testing.T) {
	for _, mode := range []string{"intent", "completion", "attempt", "generation", "tenant", "manifest", "profile", "projection identity", "envelope digest", "publication dependency", "artifact audience"} {
		t.Run(mode, func(t *testing.T) {
			r, a, p, projection := replyCredentialFixture(t)
			client := &finalCredentialClientFixture{}
			switch mode {
			case "intent":
				a.Final.IntentID = "other"
			case "completion":
				a.Final.CompletionID = "other"
			case "attempt":
				a.Final.AttemptID = "other"
			case "generation":
				a.Final.ExecutionGeneration++
			case "tenant":
				a.Final.TenantID = "other"
			case "manifest":
				p.ManifestID = "other"
			case "profile":
				p.ProfileID = "other"
			case "projection identity":
				projection.value.TenantID = "other"
			case "envelope digest":
				projection.value.EnvelopeDigest = domain.Digest([]byte("other"))
			case "publication dependency":
				projection.err = errors.New("PG unavailable")
			case "artifact audience":
				p.Artifact.AccessKeyID.AudienceDigest = domain.Digest([]byte("other"))
			}
			out, e := (replyArtifactCredentialResolver{projection: projection, client: client}).ResolveReplyArtifact(context.Background(), r, a, p)
			if e == nil || out != (artifactstore.Credentials{}) || client.calls != 0 {
				t.Fatal("untrusted request reached Control", e)
			}
		})
	}
}

type callbackProofLedger struct{ final domain.Final }

func (s callbackProofLedger) ActiveToken(context.Context, string, string, string, string) (domain.Grant, error) {
	panic("completed download must not call active token")
}
func (s callbackProofLedger) Final(context.Context, string) (domain.Final, error) {
	return s.final, nil
}

// Three authenticated HTTP legs run with one slot per class: Gateway download,
// Worker credential resolution, Control callback to Worker committed Final.
// A second download remains rejected while the first holds its download slot.
func TestReplyArtifactMaxConcurrentOneRealMTLSCallback(t *testing.T) {
	const control = "spiffe://agent-platform/control-api"
	const gateway = "spiffe://agent-platform/channel-gateway"
	const worker = "spiffe://agent-platform/agent-worker"
	r, accepted, plan, projection := replyCredentialFixture(t)
	pki := newPKI(t)
	workerServerCert, _, _ := pki.issue(t, "worker-server", "", x509.ExtKeyUsageServerAuth)
	controlServerCert, _, _ := pki.issue(t, "control-server", "", x509.ExtKeyUsageServerAuth)
	client := func(name, id string) *http.Client {
		cert, _, _ := pki.issue(t, name, id, x509.ExtKeyUsageClientAuth)
		tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.roots, Certificates: []tls.Certificate{cert}}}
		t.Cleanup(tr.CloseIdleConnections)
		return &http.Client{Transport: tr, Timeout: 3 * time.Second}
	}
	gatewayClient, controlClient, workerClient := client("gateway", gateway), client("control", control), client("worker", worker)
	var workerURL string
	callbackStatus := make(chan int, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var controlCalls atomic.Int64
	controlServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		controlCalls.Add(1)
		if request.URL.Path != finalartifacthttp.ResolvePath || request.TLS == nil || len(request.TLS.VerifiedChains) == 0 || request.TLS.VerifiedChains[0][0].URIs[0].String() != worker {
			w.WriteHeader(403)
			return
		}
		var input finalartifacthttp.Request
		if json.NewDecoder(request.Body).Decode(&input) != nil {
			w.WriteHeader(400)
			return
		}
		if input.Final.IntentID != accepted.Final.IntentID || input.Final.Digest != accepted.Final.Digest || input.DeploymentID != "dpl_fixture_deployment" {
			w.WriteHeader(403)
			return
		}
		body, _ := proof.EncodeFinalRequest(input.Final)
		response, e := controlClient.Post(workerURL+proof.FinalVerifyPath, "application/json", bytes.NewReader(body))
		if e != nil {
			callbackStatus <- 0
			w.WriteHeader(503)
			return
		}
		raw, e := io.ReadAll(response.Body)
		response.Body.Close()
		callbackStatus <- response.StatusCode
		proven, decodeErr := proof.DecodeFinalResponse(raw)
		if e != nil || decodeErr != nil || response.StatusCode != 200 || proven.FinalRequest != input.Final || proven.TenantID != input.TenantID || proven.ManifestDigest != input.ManifestDigest {
			w.WriteHeader(503)
			return
		}
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": plan.TenantID, "profile_id": plan.ProfileID, "profile_revision_number": plan.ProfileRevision, "run_id": r.RunID, "attempt_id": accepted.Final.AttemptID, "worker_id": "worker-one", "lease_epoch": 0, "manifest_id": plan.ManifestID, "manifest_digest": plan.ManifestDigest, "credentials": []any{map[string]any{"credential_id": plan.Artifact.AccessKeyID.CredentialID, "purpose": "access_key_id", "audience_digest": plan.Artifact.AccessKeyID.AudienceDigest, "credential_revision": 1, "value": "access-value"}, map[string]any{"credential_id": plan.Artifact.SecretAccessKey.CredentialID, "purpose": "secret_access_key", "audience_digest": plan.Artifact.SecretAccessKey.AudienceDigest, "credential_revision": 1, "value": "secret-value"}}})
	}))
	controlServer.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{controlServerCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	controlServer.StartTLS()
	defer func() { unblock(); controlServer.Close() }()
	resolver, e := finalartifacthttp.New(finalartifacthttp.Options{BaseURL: controlServer.URL, WorkerID: "worker-one", Client: workerClient, Timeout: 2 * time.Second, MaxResponseBytes: 8192})
	if e != nil {
		t.Fatal(e)
	}
	_, _, _, _, _, store := replyQueryFixture(t)
	query := replyArtifactQueries{ledger: &replyLedgerFixture{value: accepted}, manifests: &replyPlanFixture{plan: plan}, credentials: replyArtifactCredentialResolver{projection: projection, client: resolver}, open: func(_ context.Context, _ *pgxpool.Pool, _ datav1.Snapshot, secrets artifactstore.Credentials, tenant string, scope artifact.SessionInfo) (replyArtifactStore, error) {
		if secrets.AccessKeyID != "access-value" || secrets.SecretAccessKey != "secret-value" || tenant != plan.TenantID {
			return nil, errors.New("scope mismatch")
		}
		return store, nil
	}}
	proofQuery := proofQueries{ledger: callbackProofLedger{final: accepted.Final}}
	handler, e := httpadapter.New(proofQuery, proofQuery, httpadapter.Options{ControlPrincipals: []string{control}, GatewayPrincipals: []string{gateway}, ReplyArtifacts: query, Timeout: 2 * time.Second, MaxConcurrent: 1})
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{workerServerCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.roots}
	server.StartTLS()
	workerURL = server.URL
	defer func() { unblock(); server.Close() }()
	body, _ := proof.EncodeReplyArtifactRequest(r)
	type outcome struct {
		status  int
		body    []byte
		headers http.Header
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		response, e := gatewayClient.Post(workerURL+proof.ReplyArtifactPath, "application/json", bytes.NewReader(body))
		if e != nil {
			done <- outcome{err: e}
			return
		}
		raw, e := io.ReadAll(response.Body)
		response.Body.Close()
		done <- outcome{response.StatusCode, raw, response.Header, e}
	}()
	select {
	case status := <-callbackStatus:
		if status != 200 {
			t.Fatalf("nested Final proof failed with download slot held: %d", status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nested proof did not complete")
	}
	response, e := gatewayClient.Post(workerURL+proof.ReplyArtifactPath, "application/json", bytes.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("second download bypassed one-slot budget", response.StatusCode)
	}
	unblock()
	result := <-done
	if result.err != nil || result.status != 200 || !bytes.Equal(result.body, []byte("accepted artifact\n中\x00")) || result.headers.Get("X-Content-SHA256") != accepted.Attachment.SHA256 || controlCalls.Load() != 1 || store.calls != 1 || store.closed != 1 {
		t.Fatal("completed download failed", result.status, result.err, controlCalls.Load(), store.calls, store.closed)
	}
	// A CA-valid Gateway certificate does not acquire the Worker's credential
	// authority, even with the same persisted Final/publication request.
	wrongClient, e := finalartifacthttp.New(finalartifacthttp.Options{BaseURL: controlServer.URL, WorkerID: "worker-one", Client: gatewayClient, Timeout: time.Second, MaxResponseBytes: 8192})
	if e != nil {
		t.Fatal(e)
	}
	wrong := replyArtifactCredentialResolver{projection: projection, client: wrongClient}
	denied, e := wrong.ResolveReplyArtifact(context.Background(), r, accepted, plan)
	if !errors.Is(e, finalartifacthttp.ErrDenied) || denied != (artifactstore.Credentials{}) || controlCalls.Load() != 2 || store.calls != 1 {
		t.Fatal("wrong mTLS principal received credentials", e)
	}
}
