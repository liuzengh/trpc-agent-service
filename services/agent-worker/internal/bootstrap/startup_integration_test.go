package bootstrap

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5/pgxpool"
	events "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	runwire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	exportwire "github.com/liuzengh/trpc-agent-service/api/runtime/control/v1"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/natsadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"github.com/nats-io/nats.go"
)

type testPKI struct {
	dir    string
	ca     *x509.Certificate
	key    ed25519.PrivateKey
	roots  *x509.CertPool
	serial int64
}

func newPKI(t *testing.T) *testPKI {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Worker bootstrap fixture CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	p := &testPKI{dir: t.TempDir(), ca: ca, key: key, roots: x509.NewCertPool(), serial: 1}
	p.roots.AddCert(ca)
	os.WriteFile(filepath.Join(p.dir, "ca.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600)
	return p
}
func (p *testPKI) issue(t *testing.T, name, identity string, usage x509.ExtKeyUsage) (tls.Certificate, string, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p.serial++
	now := time.Now()
	leaf := &x509.Certificate{SerialNumber: big.NewInt(p.serial), Subject: pkix.Name{CommonName: name}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	if identity != "" {
		u, _ := url.Parse(identity)
		leaf.URIs = []*url.URL{u}
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, p.ca, pub, p.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(p.dir, name+".crt"), filepath.Join(p.dir, name+".key")
	os.WriteFile(certPath, certBytes, 0600)
	os.WriteFile(keyPath, keyBytes, 0600)
	return cert, certPath, keyPath
}
func waitUntil(t *testing.T, ctx context.Context, predicate func() bool) {
	t.Helper()
	for !predicate() {
		select {
		case <-ctx.Done():
			t.Fatal("fixture condition deadline")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Uses pre-provisioned V1 topology and schemas. It creates only unique logical
// IDs, never truncates business tables and never creates/deletes broker streams.
func TestWorkerBootstrapRealPGNATSMTLSAndDrain(t *testing.T) {
	keys := []string{"WORKER_TEST_MIGRATION_URL", "WORKER_TEST_RUNTIME_URL", "WORKER_SESSION_TEST_MIGRATION_URL", "WORKER_SESSION_TEST_RUNTIME_URL", "WORKER_BOOTSTRAP_TEST_NATS_URL"}
	for _, key := range keys {
		if os.Getenv(key) == "" {
			t.Skip("requires explicit isolated Worker bootstrap PG/NATS fixture")
		}
	}
	ctx, stop := context.WithTimeout(context.Background(), 45*time.Second)
	defer stop()
	sessionMigration, err := pgxpool.New(ctx, os.Getenv(keys[2]))
	if err != nil {
		t.Fatal(err)
	}
	defer sessionMigration.Close()
	if err = sessionmigrations.Apply(ctx, sessionMigration); err != nil {
		t.Fatal(err)
	}
	c := testConfig(t)
	c.DatabaseURL = os.Getenv(keys[1])
	c.MigrationDatabaseURL = os.Getenv(keys[0])
	c.HealthAddress = "127.0.0.1:0"
	c.InternalAddress = "127.0.0.1:0"
	c.Policy.LeaseTTL = Duration(3 * time.Second)
	c.Policy.RenewalInterval = Duration(time.Second)
	c.Timing.PollInterval = Duration(20 * time.Millisecond)
	c.Timing.HealthInterval = Duration(100 * time.Millisecond)
	c.Timing.ShutdownDrainTimeout = Duration(10 * time.Second)
	c.Timing.ShutdownCancelTimeout = Duration(5 * time.Second)
	c.Timing.StartupTimeout = Duration(20 * time.Second)
	suffix := fmt.Sprint(time.Now().UnixNano())
	c.WorkerID = "bootstrap-" + suffix
	pki := newPKI(t)
	serverCert, serverCertPath, serverKeyPath := pki.issue(t, "worker-server", "", x509.ExtKeyUsageServerAuth)
	_ = serverCert
	_, workerCertPath, workerKeyPath := pki.issue(t, "worker-client", "spiffe://agent-platform/agent-worker", x509.ExtKeyUsageClientAuth)
	controlServer, _, _ := pki.issue(t, "control-server", "", x509.ExtKeyUsageServerAuth)
	controlClient, _, _ := pki.issue(t, "control-client", c.ControlPrincipals[0], x509.ExtKeyUsageClientAuth)
	c.ProofTLS = ServerTLS{CertFile: serverCertPath, KeyFile: serverKeyPath, ClientCAFile: filepath.Join(pki.dir, "ca.crt")}
	c.ControlTLS = ClientTLS{CertFile: workerCertPath, KeyFile: workerKeyPath, CAFile: filepath.Join(pki.dir, "ca.crt")}
	password := func(user string) string {
		if value := os.Getenv("NATS_" + strings.ToUpper(user) + "_PASSWORD"); value != "" {
			return value
		}
		return "fixture-only"
	}
	c.NATSFile = filepath.Join(pki.dir, "nats.json")
	natsRaw, _ := json.Marshal(natsConfig{URL: os.Getenv(keys[4]), User: "worker", Password: password("worker")})
	os.WriteFile(c.NATSFile, natsRaw, 0600)
	connect := func(user string) *nats.Conn {
		t.Helper()
		nc, e := nats.Connect(os.Getenv(keys[4]), nats.UserInfo(user, password(user)), nats.CustomInboxPrefix("_INBOX."+user))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	gatewayNATS, _ := connect("gateway").JetStream()
	controlNATS, _ := connect("control").JetStream()
	secondEntered := make(chan struct{})
	releaseModel := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseModel) })
	var modelCalls atomic.Int64
	var modelHistory atomic.Bool
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		call := modelCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-model-key" {
			http.Error(w, "denied", 403)
			return
		}
		answer := "bootstrap first"
		if call == 2 {
			modelHistory.Store(bytes.Contains(raw, []byte("bootstrap first")))
			close(secondEntered)
			select {
			case <-releaseModel:
			case <-r.Context().Done():
				return
			}
			answer = "bootstrap second"
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":%q},\"finish_reason\":null}]}\n\n", answer)
		fmt.Fprint(w, "data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer model.Close()
	raw, err := os.ReadFile("../../../../api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.DecodeRuntimeManifestPublishedEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	content, err := protocol.VerifyRuntimeManifest(event.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	sessionConfig, err := pgxpool.ParseConfig(os.Getenv(keys[3]))
	if err != nil {
		t.Fatal(err)
	}
	modelResource := content.Resources.Models[content.AgentPlan.Nodes[content.AgentPlan.Root].ModelResource]
	modelResource.BaseURL = model.URL + "/v1"
	modelResource.Credential.AudienceDigest = protocol.CredentialAudienceDigest(modelResource.Kind, modelResource.BaseURL)
	content.Resources.Models[content.AgentPlan.Nodes[content.AgentPlan.Root].ModelResource] = modelResource
	session := content.Resources.Storage[content.StorageRoles["session"]]
	session.Destination = protocol.StorageDestination{Host: sessionConfig.ConnConfig.Host, Port: int64(sessionConfig.ConnConfig.Port), Database: sessionConfig.ConnConfig.Database, Username: sessionConfig.ConnConfig.User, SSLMode: "disable"}
	session.Credential.AudienceDigest = protocol.CredentialAudienceDigest(session.Kind, session.Destination)
	content.Resources.Storage[content.StorageRoles["session"]] = session
	content.Execution.AllowedEndpointHosts = []string{"127.0.0.1"}
	contentBytes, _ := json.Marshal(content)
	contentBytes, _ = jcs.Transform(contentBytes)
	event.Manifest.Content = contentBytes
	event.Manifest.ContentDigest = domain.Digest(contentBytes)
	event.EventID = "event_bootstrap_" + suffix
	event.Manifest.ID = "manifest_bootstrap_" + suffix
	event.DeploymentRevisionID = "revision_bootstrap_" + suffix
	event.Manifest.DeploymentRevisionID = event.DeploymentRevisionID
	event.Manifest.PublishedAt = time.Now().UTC()
	event.OccurredAt = event.Manifest.PublishedAt
	raw, _ = json.Marshal(event)
	c.PlatformContractDigest = content.PlatformContract.Digest
	exportEntered := make(chan struct{})
	releaseExport := make(chan struct{})
	var exportOnce sync.Once
	var exportReleaseOnce sync.Once
	defer exportReleaseOnce.Do(func() { close(releaseExport) })
	var appRef atomic.Pointer[App]
	var lastProof proof.AttemptRequest
	var proofMu sync.Mutex
	var proofChecks atomic.Int64
	proofHTTP := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pki.roots, Certificates: []tls.Certificate{controlClient}}}, Timeout: 2 * time.Second}
	defer proofHTTP.CloseIdleConnections()
	queryProof := func(request proof.AttemptRequest) (proof.AttemptResponse, int) {
		app := appRef.Load()
		if app == nil {
			return proof.AttemptResponse{}, 503
		}
		data, _ := proof.EncodeAttemptRequest(request)
		response, e := proofHTTP.Post("https://"+app.InternalAddress()+proof.AttemptVerifyPath, "application/json", bytes.NewReader(data))
		if e != nil {
			return proof.AttemptResponse{}, 503
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		value, _ := proof.DecodeAttemptResponse(body)
		return value, response.StatusCode
	}
	control := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0][0].URIs) != 1 || r.TLS.VerifiedChains[0][0].URIs[0].String() != "spiffe://agent-platform/agent-worker" {
			http.Error(w, "denied", 403)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == exportwire.ManifestExportPath {
			exportOnce.Do(func() { close(exportEntered) })
			select {
			case <-releaseExport:
			case <-r.Context().Done():
				return
			}
			json.NewEncoder(w).Encode(exportwire.ManifestExportPage{SchemaVersion: "v1", SnapshotUpper: &exportwire.ExportPosition{CreatedAt: event.OccurredAt, TenantID: event.TenantID, EventID: event.EventID}, Events: []json.RawMessage{raw}, Complete: true})
			return
		}
		if r.URL.Path != "/internal/v1/runtime-profiles/credentials/resolve" {
			http.NotFound(w, r)
			return
		}
		var input struct {
			ExecutionToken string                `json:"execution_token"`
			ManifestID     string                `json:"manifest_id"`
			ManifestDigest string                `json:"manifest_digest"`
			Uses           []proof.CredentialUse `json:"uses"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			http.Error(w, "bad", 400)
			return
		}
		request := proof.AttemptRequest{WorkloadIdentity: c.WorkerID, ExecutionToken: input.ExecutionToken, ManifestID: input.ManifestID, ManifestDigest: input.ManifestDigest}
		proofMu.Lock()
		lastProof = request
		proofMu.Unlock()
		grant, status := queryProof(request)
		if status != 200 {
			w.WriteHeader(status)
			return
		}
		proofChecks.Add(1)
		credentials := []map[string]any{}
		for _, use := range grant.AllowedUses {
			value := "fixture-model-key"
			if use.Purpose == "dsn" {
				value = sessionConfig.ConnConfig.Password
			}
			credentials = append(credentials, map[string]any{"credential_id": use.CredentialID, "purpose": use.Purpose, "audience_digest": use.AudienceDigest, "credential_revision": 1, "value": value})
		}
		json.NewEncoder(w).Encode(map[string]any{"tenant_id": grant.TenantID, "profile_id": grant.ProfileID, "profile_revision_number": grant.ProfileRevisionNumber, "run_id": grant.RunID, "attempt_id": grant.AttemptID, "worker_id": grant.WorkerID, "lease_epoch": grant.LeaseEpoch, "manifest_id": grant.ManifestID, "manifest_digest": grant.ManifestDigest, "credentials": credentials})
	}))
	control.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{controlServer}, ClientCAs: pki.roots, ClientAuth: tls.RequireAndVerifyClientCert}
	control.StartTLS()
	defer control.Close()
	c.ControlURL = control.URL
	app, err := New(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	appRef.Store(app)
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	done := make(chan error, 1)
	go func() { done <- app.Run(runCtx) }()
	select {
	case <-exportEntered:
	case err := <-done:
		t.Fatalf("startup failed: %v", err)
	case <-ctx.Done():
		t.Fatal("export not started")
	}
	readyStatus := func() int {
		response, e := http.Get("http://" + app.HealthAddress() + "/readyz")
		if e != nil {
			return 0
		}
		response.Body.Close()
		return response.StatusCode
	}
	if readyStatus() != 503 {
		t.Fatal("ready before owner export")
	}
	increment := event
	increment.EventID = "increment_bootstrap_" + suffix
	incrementRaw, _ := json.Marshal(increment)
	if _, err = controlNATS.Publish(events.ManifestSubject, incrementRaw, nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	exportReleaseOnce.Do(func() { close(releaseExport) })
	waitUntil(t, ctx, app.Ready)
	var projected bool
	if err = app.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manifest_receipts WHERE event_id=$1)`, increment.EventID).Scan(&projected); err != nil || !projected {
		t.Fatal("ready before durable increment catch-up")
	}
	publish := func(i int) string {
		id := fmt.Sprintf("run_bootstrap_%s_%d", suffix, i)
		r := dto.RunRequested{SchemaVersion: 1, EventID: "event_" + id, AdmissionID: "admission_" + id, RunID: id, Route: dto.RouteSnapshot{Provider: "telegram", AccountID: "bootstrap_account", Generation: 1, TenantID: event.TenantID, BindingID: "binding_" + suffix, DeploymentRevisionID: event.DeploymentRevisionID, ManifestRef: event.Manifest.ID, ManifestDigest: event.Manifest.ContentDigest}, Input: dto.Inbound{Key: dto.EventKey{Provider: "telegram", AccountID: "bootstrap_account", EventID: fmt.Sprint(i)}, Kind: "text", ConversationID: "42", SenderID: "43", Text: fmt.Sprintf("bootstrap round %d", i), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano), ReplyContext: &dto.ReplyContext{ChatID: "42"}, SourceDigest: strings.Repeat("a", 64)}}
		data, e := runwire.EncodeRunRequested(r)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = gatewayNATS.Publish(natsadapter.RunSubject, data, nats.Context(ctx)); e != nil {
			t.Fatal(e)
		}
		return id
	}
	first := publish(1)
	waitUntil(t, ctx, func() bool {
		completion, e := app.ledger.FindCompletion(ctx, event.TenantID, first)
		return e == nil && completion.Status == domain.Succeeded
	})
	second := publish(2)
	select {
	case <-secondEntered:
	case <-ctx.Done():
		t.Fatal("second model call missing")
	}
	if !modelHistory.Load() {
		t.Fatal("accepted Session missing from second SDK request")
	}
	cancelRun()
	waitUntil(t, ctx, func() bool { return app.draining.Load() })
	if readyStatus() != 503 {
		t.Fatal("draining still ready")
	}
	time.Sleep(3500 * time.Millisecond)
	proofMu.Lock()
	request := lastProof
	proofMu.Unlock()
	if _, status := queryProof(request); status != 200 {
		t.Fatalf("renewal/proof stopped during drain: %d", status)
	}
	releaseOnce.Do(func() { close(releaseModel) })
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Worker did not drain")
	}
	check, err := pgxpool.New(ctx, os.Getenv(keys[1]))
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var success, published bool
	err = check.QueryRow(ctx, `SELECT r.status='SUCCEEDED',o.published_at IS NOT NULL FROM execution_runs r JOIN execution_reply_outbox o ON o.tenant_id=r.tenant_id AND o.run_id=r.run_id WHERE r.tenant_id=$1 AND r.run_id=$2`, event.TenantID, second).Scan(&success, &published)
	if err != nil || !success || !published {
		t.Fatalf("drain did not preserve Completion/Reply: %v", err)
	}
	if modelCalls.Load() != 2 || proofChecks.Load() != 2 {
		t.Fatalf("calls model=%d resolve=%d", modelCalls.Load(), proofChecks.Load())
	}
	t.Log("BOOTSTRAP PASS: owner export before READY; retained increment catch-up; two SDK/Session rounds; authenticated online proofs; drain keeps renewal then commits and publishes Final")
}
