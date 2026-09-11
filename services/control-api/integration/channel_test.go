package integration_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/credentialcrypto"
	channelpg "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/postgres"
	channelapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	channeldomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity"
	identitypg "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant"
	tenantpg "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/outbound/postgres"
)

const channelEpoch = "00000000-0000-4000-8000-000000000001"
const channelPrincipal = "spiffe://trpc-agent-service/gateway/gw-1"

type channelCommitAuth struct{}

func (channelCommitAuth) AuthorizeOwner(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	ok, err := (identitypg.TransactionAuthorizer{}).AuthorizeSession(ctx, tx, user)
	if err != nil || !ok {
		return ok, err
	}
	return (tenantpg.TransactionAuthorizer{}).AuthorizeOwner(ctx, tx, tenant, user)
}
func (channelCommitAuth) LockRequesterUsers(ctx context.Context, tx pgx.Tx, users []string) error {
	for _, user := range users {
		if _, err := (identitypg.TransactionAuthorizer{}).AuthorizeActiveUser(ctx, tx, user); err != nil {
			return err
		}
	}
	return nil
}
func (channelCommitAuth) AuthorizeRequester(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	ok, err := (identitypg.TransactionAuthorizer{}).AuthorizeActiveUser(ctx, tx, user)
	if err != nil || !ok {
		return ok, err
	}
	return (tenantpg.TransactionAuthorizer{}).AuthorizeOwner(ctx, tx, tenant, user)
}
func (channelCommitAuth) AuthorizeActiveTenant(ctx context.Context, tx pgx.Tx, tenant string) (bool, error) {
	return (tenantpg.TransactionAuthorizer{}).AuthorizeActiveTenant(ctx, tx, tenant)
}
func composeChannel(t *testing.T, ctx context.Context, pool *pgxpool.Pool, router *gin.Engine, identity *identity.Module, tenant *tenant.Module, deployment *deployment.Module) *channelbinding.Module {
	t.Helper()
	key := make([]byte, 32)
	mac := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(mac); err != nil {
		t.Fatal(err)
	}
	cipher, err := credentialcrypto.New("test-key", map[string]credentialcrypto.Key{"test-key": {Encryption: key, MAC: mac}})
	clear(key)
	clear(mac)
	if err != nil {
		t.Fatal(err)
	}
	module, err := channelbinding.NewModule(channelbinding.Dependencies{DB: pool, Routes: router, Authenticate: identity.AuthenticationMiddleware(), TenantAccess: tenantAccess{tenants: tenant.Service}, TransactionAuthorizer: channelCommitAuth{}, Deployments: deployment.Service, Cipher: cipher, Options: channelpg.Options{ScopeID: "gateway_pool", SourceEpoch: channelEpoch}, Workloads: []channelapp.WorkloadPrincipal{{PrincipalID: channelPrincipal, ScopeID: "gateway_pool", InstanceID: "gw-1", Audience: channelapp.WorkloadAudience, Consumers: []string{"telegram_registration", "telegram_webhook", "telegram_delivery", "telegram_preflight"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = module.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	return module
}
func testChannelHTTP(t *testing.T, ctx context.Context, router http.Handler, pool *pgxpool.Pool, module *channelbinding.Module, tenant string, owner, member, outsider *http.Cookie) {
	base := "/v1/tenants/" + tenant + "/channel-accounts"
	body := `{"provider":"telegram","config":{"receive_mode":"webhook"},"provider_account_id":"00987654","name":"HTTP test","credentials":{"telegram.bot_token":{"action":"replace","value":"TEST_ONLY_BOT_TOKEN"},"telegram.webhook_secret":{"action":"replace","value":"TEST_ONLY_WEBHOOK_SECRET"}}}`
	// Real session middleware and owner use case precede body parsing.
	deploymentRequest(t, router, "POST", base, nil, "missing-session", body, 401, nil)
	deploymentRequest(t, router, "POST", base, member, "member-denied", "{invalid", 403, nil)
	deploymentRequest(t, router, "POST", base, outsider, "outsider-denied", body, 403, nil)
	var created channelapp.CommandResult
	first := deploymentRequest(t, router, "POST", base, owner, "channel-http-create", body, 201, &created)
	replay := deploymentRequest(t, router, "POST", base, owner, "channel-http-create", body, 201, nil)
	if first.Body.String() != replay.Body.String() {
		t.Fatal("account receipt changed")
	}
	assertChannelPublic(t, first.Body.Bytes())
	if created.Account == nil || created.Account.ID == "" || created.Account.ProviderAccountID != "987654" || created.Account.Enabled {
		t.Fatal("incorrect initial account state")
	}
	id := created.Account.ID
	path := base + "/" + id
	deploymentRequest(t, router, "POST", base, owner, "channel-http-create", strings.Replace(body, "HTTP test", "Changed", 1), 409, nil)
	deploymentRequest(t, router, "POST", base, owner, "channel-duplicate-physical", body, 409, nil)
	deploymentRequest(t, router, "POST", base, owner, "channel-unknown", strings.Replace(body, `"name":`, `"unknown":true,"name":`, 1), 400, nil)
	request(t, router, "GET", path, member, "", 200, nil)
	request(t, router, "GET", base+"?page_size=1&page_size=2", member, "", 400, nil)
	request(t, router, "GET", base+"?page_size=101", member, "", 400, nil)
	request(t, router, "GET", base+"?unknown=1", member, "", 400, nil)
	var page channelapp.AccountPage
	request(t, router, "GET", base+"?page_size=1", member, "", 200, &page)
	if len(page.Accounts) != 1 {
		t.Fatal("account page missing")
	}
	request(t, router, "GET", "/internal/v1/channel-accounts/snapshot", owner, "", 404, nil)
	// Compile a real immutable publication through Agent, Profile and Deployment APIs.
	agentID := createDeploymentAgentVersion(t, router, tenant, owner)
	profileID := createDeploymentProfileRevision(t, router, tenant, owner, deploymentProfileConfig("channel-model", "matching"))
	deployments := "/v1/tenants/" + tenant + "/deployments"
	var deploymentCreated struct {
		Deployment deploymentHTTPView `json:"deployment"`
	}
	deploymentRequest(t, router, "POST", deployments, owner, "channel-deployment", `{"name":"Channel publication"}`, 201, &deploymentCreated)
	input := deploymentInputJSON(t, agentID, 1, profileID, 1)
	var published deploymentPublishHTTPView
	deploymentRequest(t, router, "POST", deployments+"/"+deploymentCreated.Deployment.ID+"/revisions", owner, "channel-publish", `{"expected_latest_revision_number":null,"input":`+input+`}`, 201, &published)
	bindingBase := "/v1/tenants/" + tenant + "/channel-bindings"
	bindingBody, _ := json.Marshal(map[string]any{"account_id": id, "target": map[string]any{"deployment_id": deploymentCreated.Deployment.ID, "revision_number": 1}})
	var binding channelapp.CommandResult
	deploymentRequest(t, router, "POST", bindingBase, owner, "channel-bind", string(bindingBody), 201, &binding)
	if binding.Binding == nil || binding.Binding.Target.DeploymentRevisionID != published.Revision.ID || binding.Binding.Target.ManifestDigest != published.Revision.ManifestDigest {
		t.Fatal("binding did not freeze the owner-verified publication")
	}
	var enabled channelapp.CommandResult
	deploymentRequest(t, router, "POST", path+"/enabled", owner, "channel-enable", `{"expected_account_revision":1,"enabled":true}`, 200, &enabled)
	var route channelapp.CommandResult
	deploymentRequest(t, router, "POST", bindingBase+"/"+binding.Binding.ID+"/enabled", owner, "channel-route", `{"expected_binding_revision":1,"enabled":true}`, 200, &route)
	if route.EventID == "" || route.Distribution != "PENDING" {
		t.Fatal("route event not staged")
	}
	var digest, manifest string
	if err := pool.QueryRow(ctx, `SELECT payload_jsonb->'route'->>'manifest_digest',payload_jsonb->'route'->>'manifest_ref' FROM control_outbox WHERE tenant_id=$1 AND id=$2`, tenant, route.EventID).Scan(&digest, &manifest); err != nil {
		t.Fatal(err)
	}
	if digest != published.Revision.ManifestDigest || manifest != binding.Binding.Target.ManifestID {
		t.Fatal("outbox target differs from immutable manifest")
	}
	var details channelapp.AccountDetails
	request(t, router, "GET", path, member, "", 200, &details)
	if details.Distribution != "PENDING" || details.GatewayApplication != "UNKNOWN" {
		t.Fatal("distribution falsely claims application")
	}
	raw, _ := json.Marshal(details)
	assertChannelPublic(t, raw)
	testChannelMTLS(t, router, module.InternalHandler, tenant, id, path, owner)
}
func assertChannelPublic(t *testing.T, raw []byte) {
	t.Helper()
	for _, needle := range []string{"TEST_ONLY_", `"credential_id"`, `"key_id"`, `"ciphertext"`, `"value"`, `"scope_id"`} {
		if bytes.Contains(raw, []byte(needle)) {
			t.Fatalf("public response contains forbidden marker %q", needle)
		}
	}
}

// Certificates are locally generated fixtures; requests still complete a real TLS handshake.
func channelTLS(t *testing.T) (*tls.Config, *tls.Config, *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Channel test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
	issue := func(serial int64, uri string, server bool) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature}
		if server {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		} else {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
			u, err := url.Parse(uri)
			if err != nil {
				t.Fatal(err)
			}
			cert.URIs = []*url.URL{u}
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	}
	server := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{issue(2, "", true)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	client := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{issue(3, channelPrincipal, false)}}
	denied := client.Clone()
	denied.Certificates = []tls.Certificate{issue(4, "spiffe://trpc-agent-service/gateway/unknown", false)}
	return server, client, denied
}
func testChannelMTLS(t *testing.T, router, internal http.Handler, tenant, id, publicPath string, owner *http.Cookie) {
	serverTLS, clientTLS, deniedTLS := channelTLS(t)
	server := httptest.NewUnstartedServer(internal)
	server.TLS = serverTLS
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	transport := &http.Transport{TLSClientConfig: clientTLS}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	call := func(method, path string, body any, want int) []byte {
		t.Helper()
		var raw []byte
		if body != nil {
			var err error
			raw, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != want {
			t.Fatalf("%s %s status=%d want=%d", method, path, res.StatusCode, want)
		}
		if res.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("internal response cacheable")
		}
		return data
	}
	snapshotPath := "/internal/v1/channel-accounts/snapshot"
	raw := call("GET", snapshotPath, nil, 200)
	if err := channelv1.Validate("account-snapshot.schema.json", raw); err != nil {
		t.Fatal(err)
	}
	var snapshot channeldomain.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	var account channeldomain.SnapshotAccount
	for _, a := range snapshot.Accounts {
		if a.AccountID == id {
			account = a
		}
	}
	if account.AccountID == "" || !account.Enabled {
		t.Fatal("snapshot missing enabled account")
	}
	epoch := int64(1)
	requestBody := channelapp.ResolveRequest{SchemaVersion: 1, ScopeID: "gateway_pool", SourceEpoch: channelEpoch, ConnectionRevision: account.ConnectionRevision, Consumer: channeldomain.Consumer{Kind: "telegram_registration", InstanceID: "gw-1", RegistrationEpoch: &epoch}}
	for _, credential := range account.Credentials {
		requestBody.Uses = append(requestBody.Uses, channeldomain.CredentialUse{Purpose: credential.Purpose, ID: credential.ID, Version: credential.Version})
	}
	resolvePath := "/internal/v1/tenants/" + tenant + "/channel-accounts/" + id + "/credentials:resolve"
	raw = call("POST", resolvePath, requestBody, 200)
	if err := channelv1.Validate("credentials-resolve-response.schema.json", raw); err != nil {
		t.Fatal(err)
	}
	var resolved channelapp.ResolveResponse
	if err := json.Unmarshal(raw, &resolved); err != nil {
		t.Fatal(err)
	}
	if len(resolved.Values) != 2 || resolved.ConnectionRevision != account.ConnectionRevision {
		t.Fatal("resolve not same-version complete")
	}
	for _, v := range resolved.Values {
		if !strings.HasPrefix(v.Value, "TEST_ONLY_") {
			t.Fatal("unexpected fixture value")
		}
	}
	wrong := requestBody
	wrong.Consumer.InstanceID = "gw-spoof"
	call("POST", resolvePath, wrong, 403)
	call("POST", strings.Replace(resolvePath, tenant, "different-tenant", 1), requestBody, 404)
	call("GET", snapshotPath+"?scope_id=spoof", nil, 400)
	observation := channelapp.ObservationsRequest{SchemaVersion: 1, Observations: []channelapp.Observation{{ScopeID: "gateway_pool", SourceEpoch: channelEpoch, TenantID: tenant, AccountID: id, Provider: channeldomain.Telegram, ConnectionRevision: account.ConnectionRevision, InstanceID: "gw-1", InstanceEpoch: channelEpoch, ReportSequence: 1, State: "CONFIG_APPLIED", ReasonCode: "NONE", ObservedAt: time.Now().UTC()}}}
	call("POST", "/internal/v1/channel-account-observations", observation, 204)
	var details channelapp.AccountDetails
	request(t, router, "GET", publicPath, owner, "", 200, &details)
	if len(details.Observations) != 1 || details.Observations[0].EffectiveState != "CONFIG_APPLIED" {
		t.Fatal("observation not reflected")
	}
	// A valid client certificate with an unconfigured URI is still denied.
	deniedTransport := &http.Transport{TLSClientConfig: deniedTLS}
	defer deniedTransport.CloseIdleConnections()
	deniedClient := &http.Client{Transport: deniedTransport, Timeout: 5 * time.Second}
	req, _ := http.NewRequest("GET", server.URL+snapshotPath, nil)
	req.Header.Set("X-Workload-Principal", channelPrincipal)
	req.AddCookie(owner)
	res, err := deniedClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 403 {
		t.Fatal("header or cookie overrode certificate mapping")
	}
	noCert := clientTLS.Clone()
	noCert.Certificates = nil
	noCertTransport := &http.Transport{TLSClientConfig: noCert}
	defer noCertTransport.CloseIdleConnections()
	noCertClient := &http.Client{Transport: noCertTransport, Timeout: 5 * time.Second}
	if res, err := noCertClient.Get(server.URL + snapshotPath); err == nil {
		res.Body.Close()
		t.Fatal("mTLS accepted no client certificate")
	}
	plain := httptest.NewRecorder()
	plainReq := httptest.NewRequest("GET", snapshotPath, nil)
	plainReq.Header.Set("X-Workload-Principal", channelPrincipal)
	plainReq.AddCookie(owner)
	internal.ServeHTTP(plain, plainReq)
	if plain.Code != 401 {
		t.Fatal("plain transport accepted")
	}
	// Rotation changes connection revision, rejects old snapshot references, and redacts public result.
	update := `{"expected_account_revision":` + jsonNumber(details.Account.Revision) + `,"expected_credential_version":1,"action":"replace","value":"TEST_ONLY_ROTATED_SECRET"}`
	deploymentRequest(t, router, "POST", publicPath+"/credentials/telegram.webhook_secret/update", owner, "channel-rotate", update, 200, nil)
	call("POST", resolvePath, requestBody, 409)
}
func jsonNumber(v int64) string { raw, _ := json.Marshal(v); return string(raw) }
