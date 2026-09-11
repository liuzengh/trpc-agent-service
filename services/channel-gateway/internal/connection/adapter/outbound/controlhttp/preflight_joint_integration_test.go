package controlhttp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	p "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

// TestPreflightGatewayClientAgainstControlBinary crosses the actual process and
// module boundaries: public Session/OWNER APIs -> real Control PostgreSQL Store
// -> real mTLS Gateway adapter -> real Service -> complete -> public View.
// Only TelegramProbe is synthetic. No other service's internal package is linked.
func TestPreflightGatewayClientAgainstControlBinary(t *testing.T) {
	binary, dsn, natsFile := os.Getenv("GATEWAY_CONTROL_PREFLIGHT_BINARY"), os.Getenv("GATEWAY_TEST_DATABASE_URL"), os.Getenv("GATEWAY_CONTROL_PREFLIGHT_NATS_FILE")
	if binary == "" || dsn == "" || natsFile == "" {
		t.Skip("GATEWAY_CONTROL_PREFLIGHT_BINARY, GATEWAY_TEST_DATABASE_URL and GATEWAY_CONTROL_PREFLIGHT_NATS_FILE are required")
	}
	budget := 90 * time.Second
	if os.Getenv("GATEWAY_PREFLIGHT_BROWSER_HANDOFF") != "" {
		budget = 20 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	t.Cleanup(cancel)
	database, migrationDatabase, pool := jointPreflightDatabase(t, ctx, dsn)
	dir := t.TempDir()
	serverCert, serverKey, caFile, clientCert, roots := jointPreflightCertificates(t, dir)
	publicAddress, publicReserve := jointPreflightAddress(t)
	internalAddress, internalReserve := jointPreflightAddress(t)
	defer publicReserve.Close()
	defer internalReserve.Close()
	scope, epoch := "joint_preflight_pool", "44444444-4444-4444-8444-444444444444"
	keys := jointPreflightJSONFile(t, dir, "keys.json", map[string]any{"active_key_id": "joint-key", "keys": map[string]any{"joint-key": map[string]string{"encryption_key": jointPreflightRandomKey(t), "mac_key": jointPreflightRandomKey(t)}}})
	config := jointPreflightJSONFile(t, dir, "channel.json", map[string]any{
		"scope_id": scope, "source_epoch": epoch, "internal_address": internalAddress, "tls_cert_file": serverCert, "tls_key_file": serverKey, "client_ca_file": caFile, "credential_keys_file": keys, "route_nats_file": natsFile, "max_tenant_accounts": 20,
		"workloads": []any{map[string]any{"principal_id": "spiffe://trpc-agent-service/gateway/joint-preflight", "instance_id": "joint-preflight", "scope_id": scope, "audience": "control-channel-v1", "consumers": []string{"telegram_preflight", "telegram_registration", "wecom_preflight"}}},
	})
	env := jointPreflightProcessEnv()
	digestCommand := exec.CommandContext(ctx, binary, "-print-deployment-contract-digest")
	digestCommand.Env = env
	rawDigest, err := digestCommand.Output()
	if err != nil || len(strings.TrimSpace(string(rawDigest))) != 71 || !strings.HasPrefix(string(rawDigest), "sha256:") {
		t.Fatal("prepare isolated Control execution contract digest failed")
	}
	const initialPassword = "Joint-bootstrap-Temporary-1234"
	env = append(env, "CONTROL_DATABASE_URL="+database, "CONTROL_MIGRATION_DATABASE_URL="+migrationDatabase, "CONTROL_PROFILE_CREDENTIAL_KEY="+jointPreflightRandomKey(t), "CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST="+strings.TrimSpace(string(rawDigest)), "CONTROL_HTTP_ADDRESS="+publicAddress, "CONTROL_CHANNEL_CONFIG_FILE="+config, "CONTROL_SESSION_COOKIE_SECURE=false", "CONTROL_BOOTSTRAP_MODE=auto", "CONTROL_BOOTSTRAP_USERNAME=joint-admin", "CONTROL_BOOTSTRAP_PASSWORD="+initialPassword)
	_ = publicReserve.Close()
	_ = internalReserve.Close()
	logs := jointPreflightStartControl(t, ctx, binary, env, "http://"+publicAddress)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	public := &http.Client{Jar: jar, Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	base := "http://" + publicAddress
	login := jointPreflightPublic(t, ctx, public, base, "POST", "/v1/auth/login", "", map[string]string{"username": "joint-admin", "password": initialPassword}, 200)
	var identity struct {
		User struct {
			ID string `json:"id"`
		} `json:"user"`
		Restricted bool `json:"password_change_required"`
	}
	if json.Unmarshal(login, &identity) != nil || identity.User.ID == "" || !identity.Restricted {
		t.Fatal("bootstrap login did not return a restricted real session")
	}
	jointPreflightPublic(t, ctx, public, base, "POST", "/v1/me/change-password", "", map[string]string{"current_password": initialPassword, "new_password": "Joint-final-Password-5678"}, 204)
	tenantRaw := jointPreflightPublic(t, ctx, public, base, "POST", "/v1/admin/tenants", "", map[string]string{"slug": "joint-preflight", "name": "Joint preflight", "owner_user_id": identity.User.ID}, 201)
	var tenant struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(tenantRaw, &tenant) != nil || tenant.ID == "" {
		t.Fatal("real tenant OWNER provisioning failed")
	}
	gateway, err := NewPreflight(Options{BaseURL: "https://" + internalAddress, ScopeID: scope, SourceEpoch: epoch, InstanceID: "joint-preflight", RootCAs: roots, Certificate: clientCert})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	const token = "TEST_ONLY_GATEWAY_JOINT_BOT_TOKEN"
	const secret = "TEST_ONLY_GATEWAY_JOINT_WEBHOOK_SECRET"
	var lastClaim time.Time
	for i, tc := range []struct {
		name, mode, origin, originCode, webhookCode, outcome string
		present                                              bool
	}{
		{"invalid_origin", "webhook", "https://localhost:18443", p.OriginInvalid, "WEBHOOK_COMPARISON_UNAVAILABLE", "FAIL", true},
		{"non_public_origin", "webhook", "https://127.0.0.1", p.OriginNotPublic, "WEBHOOK_COMPARISON_UNAVAILABLE", "FAIL", true},
		{"valid_origin", "webhook", "https://gateway.example.com", p.OriginValid, "WEBHOOK_MATCH", "WARN", true},
		{"polling_no_origin", "long_polling", "", p.OriginInvalid, "WEBHOOK_NONE", "PASS", false},
		{"polling_private_origin", "long_polling", "https://127.0.0.1", p.OriginNotPublic, "WEBHOOK_NONE", "PASS", false},
		{"polling_blocked", "long_polling", "", p.OriginInvalid, "WEBHOOK_BLOCKS_LONG_POLLING", "FAIL", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credentials := map[string]any{"telegram.bot_token": map[string]string{"action": "replace", "value": token}}
			if tc.mode == "webhook" {
				credentials["telegram.webhook_secret"] = map[string]string{"action": "replace", "value": secret}
			}
			accountRaw := jointPreflightPublic(t, ctx, public, base, "POST", "/v1/tenants/"+tenant.ID+"/channel-accounts", "joint-account-"+tc.name, map[string]any{"provider": "telegram", "provider_account_id": fmt.Sprint(123456789 + i), "name": "Joint disabled " + tc.name, "config": map[string]string{"receive_mode": tc.mode}, "credentials": credentials}, 201)
			var account struct {
				Account struct {
					ID                 string `json:"account_id"`
					Enabled            bool   `json:"enabled"`
					Revision           int64  `json:"account_revision"`
					ConnectionRevision int64  `json:"connection_revision"`
					Credentials        []struct {
						Purpose    string `json:"purpose"`
						Version    int64  `json:"credential_version"`
						Configured bool   `json:"configured"`
					} `json:"credentials"`
				} `json:"account"`
			}
			if json.Unmarshal(accountRaw, &account) != nil || account.Account.ID == "" || account.Account.Enabled {
				t.Fatal("account must be saved and disabled")
			}
			var tokenVersion int64
			for _, credential := range account.Account.Credentials {
				if credential.Purpose == "telegram.bot_token" && credential.Configured {
					tokenVersion = credential.Version
				}
			}
			if tokenVersion == 0 {
				t.Fatal("configured Token metadata missing")
			}
			var bindings, deployments int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM channel_bindings WHERE account_id=$1", account.Account.ID).Scan(&bindings); err != nil || bindings != 0 {
				t.Fatal("preflight fixture must have no Binding")
			}
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM deployments").Scan(&deployments); err != nil || deployments != 0 {
				t.Fatal("preflight fixture must have no Deployment")
			}
			before := jointPreflightRuntimeHashes(t, ctx, pool)
			createdRaw := jointPreflightPublic(t, ctx, public, base, "POST", "/v1/tenants/"+tenant.ID+"/channel-accounts/"+account.Account.ID+"/preflights", "joint-create-"+tc.name, wire.PreflightCreateRequest{ExpectedAccountRevision: account.Account.Revision, ExpectedConnectionRevision: account.Account.ConnectionRevision, ExpectedBotTokenVersion: tokenVersion}, 202)
			var created wire.PreflightCreated
			if err := wire.Decode("preflight-created.schema.json", createdRaw, &created); err != nil {
				t.Fatal(err)
			}
			cfg, err := p.NewConfig(scope, epoch, tc.origin)
			if err != nil || cfg.OriginStatus != tc.originCode {
				t.Fatal("invalid diagnostic configuration", err)
			}
			claim := p.ClaimRequest{DiagnosticPolicy: wire.PreflightReceiveModesPolicy, Config: cfg, InstanceEpoch: "55555555-5555-4555-8555-555555555555", RequestID: fmt.Sprintf("66666666-6666-4666-8666-%012d", i+1), Token: p.NewSecret(base64.RawURLEncoding.EncodeToString(jointPreflightRandomBytes(t, 32)))}
			// One shared instance owns these claims; respect the real 2/s gate
			// rather than turning three fast scenarios into a quota test.
			if !lastClaim.IsZero() {
				timer := time.NewTimer(time.Until(lastClaim.Add(550 * time.Millisecond)))
				select {
				case <-ctx.Done():
					timer.Stop()
					t.Fatal("claim pacing context ended")
				case <-timer.C:
				}
			}
			lastClaim = time.Now()
			grant, err := gateway.Claim(ctx, claim)
			if err != nil || grant == nil {
				t.Fatal("real Gateway Claim -> Control handler failed", err)
			}
			if grant.PreflightID != created.PreflightID || grant.AccountID != account.Account.ID || grant.Credential.Version != tokenVersion || grant.ReceiveMode != tc.mode || grant.DiagnosticPolicy != wire.PreflightReceiveModesPolicy || grant.EffectiveConfigDigest == "" {
				t.Fatal("claim did not preserve exact task/account/version")
			}
			resolved, err := gateway.ResolveCredential(ctx, *grant)
			if err != nil || resolved.Reveal() != token {
				t.Fatal("real exact BotToken resolve failed", err)
			}
			probe := &jointPreflightProbe{token: token, identity: fmt.Sprint(123456789 + i), originValid: cfg.PublicOrigin != nil, present: tc.present, polling: tc.mode == "long_polling"}
			service := p.Service{Control: gateway, Probe: probe}
			workCtx, stopWork := context.WithTimeout(ctx, 20*time.Second)
			result, err := service.Execute(workCtx, *grant, cfg)
			stopWork()
			if err != nil || probe.calls.Load() != 1 {
				t.Fatal("real Service execution failed", err)
			}
			checkOriginCode := tc.originCode
			if tc.mode == "long_polling" {
				checkOriginCode = "PUBLIC_ORIGIN_NOT_APPLICABLE"
			}
			if len(result.Checks) != 8 || result.Checks[2].Code != checkOriginCode || result.Checks[3].Code != tc.webhookCode {
				t.Fatal("Service diagnostic branch differs from real Control contract")
			}
			if err := gateway.Complete(ctx, *grant, result); err != nil {
				t.Fatal("real Gateway Complete -> Control handler failed", err)
			}
			viewRaw := jointPreflightPublic(t, ctx, public, base, "GET", created.StatusURL, "", nil, 200)
			var view wire.PreflightView
			if err := wire.Decode("preflight-view.schema.json", viewRaw, &view); err != nil {
				t.Fatal(err)
			}
			wantOutcome := tc.outcome
			if view.State != "COMPLETED" || view.Outcome != wantOutcome || len(view.Checks) != 8 || view.GatewayConfigFreshness == nil || *view.GatewayConfigFreshness != "UNCONFIRMED" || view.Checks[2].Code != checkOriginCode || view.Checks[3].Code != tc.webhookCode {
				t.Fatal("real completed public View differs from Gateway result")
			}
			if err := gateway.Complete(ctx, *grant, result); err != nil {
				t.Fatal("same Complete replay was not accepted", err)
			}
			replayRaw := jointPreflightPublic(t, ctx, public, base, "GET", created.StatusURL, "", nil, 200)
			if !bytes.Equal(viewRaw, replayRaw) {
				t.Fatal("completed receipt replay changed stored public result or timestamps")
			}
			for _, canary := range []string{token, secret, claim.Token.Reveal()} {
				if bytes.Contains(viewRaw, []byte(canary)) || logs.contains(canary) {
					t.Fatal("secret escaped into public view or process logs")
				}
			}
			after := jointPreflightRuntimeHashes(t, ctx, pool)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("diagnostic flow modified runtime account/binding/route/catalog/credential/observation/outbox state")
			}
			t.Logf("real Control process + PostgreSQL + mTLS: %s COMPLETED, eight checks, same-complete replay, eight runtime tables unchanged", tc.name)
		})
	}
	jointWeComPreflight(t, ctx, public, base, tenant.ID, scope, epoch, gateway, pool, logs)
}

type jointPreflightProbe struct {
	token, identity  string
	originValid      bool
	present, polling bool
	calls            atomic.Int32
}

func (q *jointPreflightProbe) Inspect(ctx context.Context, r p.ProbeRequest) (p.ProbeResult, error) {
	q.calls.Add(1)
	if ctx.Err() != nil {
		return p.ProbeResult{}, p.ErrExpired
	}
	if r.Token.Reveal() != q.token || r.ExpectedIdentity != q.identity || (r.ExpectedWebhook != nil) != q.originValid {
		return p.ProbeResult{}, p.ErrInvalid
	}
	yes, count, hasError := true, int64(2), true
	if q.polling {
		count, hasError = 0, false
	}
	result := p.ProbeResult{IdentityCode: "BOT_IDENTITY_MATCH", IdentityMatch: &yes, Presence: &q.present, PendingUpdates: &count, HasLastError: &hasError}
	if hasError {
		last := time.Now().Add(-time.Minute).UTC()
		result.LastErrorAt = &last
	}
	if !q.present {
		result.WebhookCode, result.Relation = "WEBHOOK_NONE", "NONE"
	} else if q.originValid {
		result.WebhookCode = "WEBHOOK_MATCH"
		result.Relation = "MATCH"
	} else {
		result.WebhookCode = "WEBHOOK_COMPARISON_UNAVAILABLE"
		result.Relation = "UNKNOWN"
	}
	return result, nil
}

// The production process binds fixed control schema/roles. Provision a separate
// database plus new nonadministrative roles in an explicitly isolated PG17+
// test cluster; never relax the production database ownership checks.
func jointPreflightDatabase(t *testing.T, ctx context.Context, dsn string) (string, string, *pgxpool.Pool) {
	t.Helper()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("open test admin")
	}
	t.Cleanup(admin.Close)
	database := "wecom_joint_" + hex.EncodeToString(jointPreflightRandomBytes(t, 8))
	runtimePassword := hex.EncodeToString(jointPreflightRandomBytes(t, 24))
	migrationPassword := hex.EncodeToString(jointPreflightRandomBytes(t, 24))
	for _, role := range []struct{ name, password string }{{"control_migrator", migrationPassword}, {"control_runtime", runtimePassword}} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role.name}.Sanitize()+" LOGIN PASSWORD '"+role.password+"' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"); err != nil {
			t.Fatal("joint test needs its own PG cluster without existing Control roles", jointPreflightErrorClass(err))
		}
		name := role.name
		t.Cleanup(func() {
			if _, err := admin.Exec(context.Background(), "DROP ROLE "+pgx.Identifier{name}.Sanitize()); err != nil {
				t.Error("drop test role", jointPreflightErrorClass(err))
			}
		})
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
		t.Fatal("create test database")
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error("drop test database")
		}
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("test DSN")
	}
	parsed.Path = "/" + database
	query := parsed.Query()
	query.Set("search_path", "control")
	parsed.RawQuery = query.Encode()
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal("open isolated database")
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA control AUTHORIZATION control_migrator; REVOKE ALL ON SCHEMA control FROM PUBLIC; GRANT USAGE ON SCHEMA control TO control_runtime; REVOKE CREATE ON SCHEMA public FROM PUBLIC; ALTER DEFAULT PRIVILEGES FOR ROLE control_migrator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC; ALTER DEFAULT PRIVILEGES FOR ROLE control_migrator IN SCHEMA control GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO control_runtime; ALTER DEFAULT PRIVILEGES FOR ROLE control_migrator IN SCHEMA control GRANT USAGE, SELECT ON SEQUENCES TO control_runtime"); err != nil {
		t.Fatal("provision isolated database roles", jointPreflightErrorClass(err))
	}
	parsed.User = url.UserPassword("control_runtime", runtimePassword)
	runtimeURL := parsed.String()
	parsed.User = url.UserPassword("control_migrator", migrationPassword)
	return runtimeURL, parsed.String(), pool
}
func jointPreflightRuntimeHashes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	result := map[string]string{}
	for _, table := range []string{"channel_accounts", "channel_account_credentials", "channel_bindings", "channel_account_route_states", "channel_account_catalog", "channel_account_observations", "channel_command_receipts", "control_outbox"} {
		var digest string
		query := `SELECT md5(COALESCE(string_agg(to_jsonb(row)::text,',' ORDER BY to_jsonb(row)::text),'')) FROM ` + pgx.Identifier{table}.Sanitize() + ` row`
		if err := pool.QueryRow(ctx, query).Scan(&digest); err != nil {
			t.Fatal("hash isolated runtime table failed")
		}
		result[table] = digest
	}
	return result
}
func jointPreflightProcessEnv() []string {
	var result []string
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "CONTROL_") {
			result = append(result, v)
		}
	}
	return result
}

type jointPreflightLogs struct {
	mu   sync.Mutex
	body bytes.Buffer
}

func (l *jointPreflightLogs) Write(raw []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.body.Len() < 256<<10 {
		_, _ = l.body.Write(raw)
	}
	return len(raw), nil
}
func (l *jointPreflightLogs) contains(value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Contains(l.body.Bytes(), []byte(value))
}

// Emit only the startup error, never request or debug logs. URI credentials and
// long opaque key/token material are redacted even in isolated test failures.
func (l *jointPreflightLogs) failureSummary() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range strings.Split(l.body.String(), "\n") {
		if _, msg, ok := strings.Cut(line, "control-api stopped with error: "); ok {
			msg = regexp.MustCompile(`(?:postgres(?:ql)?|nats|https?)://[^\s]+`).ReplaceAllString(msg, "[REDACTED_URL]")
			msg = regexp.MustCompile(`[A-Za-z0-9_+/=-]{32,}`).ReplaceAllString(msg, "[REDACTED_OPAQUE]")
			return msg
		}
	}
	return "no stable startup error"
}
func (l *jointPreflightLogs) digest() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	sum := sha256.Sum256(l.body.Bytes())
	return hex.EncodeToString(sum[:])
}
func jointPreflightStartControl(t *testing.T, ctx context.Context, binary string, env []string, origin string) *jointPreflightLogs {
	t.Helper()
	logs := &jointPreflightLogs{}
	command := exec.CommandContext(ctx, binary)
	command.Env = env
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatal("start real Control executable failed")
	}
	done := make(chan struct{})
	var processErr error
	go func() { processErr = command.Wait(); close(done) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if err := command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Error("signal Control graceful shutdown failed")
			}
			select {
			case <-done:
				if processErr != nil {
					t.Errorf("Control exited unsuccessfully during cleanup: %v", processErr)
				}
			case <-time.After(12 * time.Second):
				_ = command.Process.Kill()
				<-done
				t.Error("Control graceful shutdown exceeded 12 seconds and required forced kill")
			}
		})
	}
	t.Cleanup(stop)
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 250 * time.Millisecond}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			t.Fatalf("Control exited before health: %v; log SHA-256 %s; %s", processErr, logs.digest(), logs.failureSummary())
		default:
		}
		request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/healthz", nil)
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == 200 || response.StatusCode == 204 {
				return logs
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("Control startup context ended")
		case <-time.After(25 * time.Millisecond):
		}
	}
	t.Fatalf("Control did not become healthy; log SHA-256 %s", logs.digest())
	return nil
}
func jointPreflightPublic(t *testing.T, ctx context.Context, client *http.Client, base, method, path, key string, body any, status int) []byte {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatal("marshal public request failed")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal("construct public request failed")
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal("real public Control request failed")
	}
	defer response.Body.Close()
	result, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(result) > 64<<10 {
		t.Fatal("invalid bounded public response")
	}
	if response.StatusCode != status {
		var problem struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(result, &problem)
		t.Fatalf("public %s %s: status %d (want %d), stable code %s", method, path, response.StatusCode, status, problem.Error.Code)
	}
	if strings.Contains(path, "/preflights") && response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("preflight public response lacks no-store")
	}
	return result
}
func jointPreflightRandomBytes(t *testing.T, n int) []byte {
	t.Helper()
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func jointPreflightRandomKey(t *testing.T) string {
	return base64.StdEncoding.EncodeToString(jointPreflightRandomBytes(t, 32))
}
func jointPreflightJSONFile(t *testing.T, dir, name string, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return jointPreflightWrite(t, dir, name, raw)
}
func jointPreflightWrite(t *testing.T, dir, name string, raw []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func jointPreflightAddress(t *testing.T) (string, net.Listener) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener.Addr().String(), listener
}
func jointPreflightCertificates(t *testing.T, dir string) (string, string, string, tls.Certificate, *x509.CertPool) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "joint-preflight-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	rootDER, err := x509.CreateCertificate(rand.Reader, root, root, public, private)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	caFile := jointPreflightWrite(t, dir, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER}))
	issue := func(serial int64, client bool) ([]byte, []byte) {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "joint-preflight"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
		if client {
			principal, _ := url.Parse("spiffe://trpc-agent-service/gateway/joint-preflight")
			leaf.URIs = []*url.URL{principal}
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		} else {
			leaf.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, private)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	}
	serverPEM, keyPEM := issue(2, false)
	clientPEM, clientKey := issue(3, true)
	certificate, err := tls.X509KeyPair(clientPEM, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return jointPreflightWrite(t, dir, "server.pem", serverPEM), jointPreflightWrite(t, dir, "server-key.pem", keyPEM), caFile, certificate, roots
}

var _ p.TelegramProbe = (*jointPreflightProbe)(nil)

// Report only stable classifications, never a URL-bearing pgconn/net error.
func jointPreflightErrorClass(err error) string {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		return "SQLSTATE=" + state.SQLState()
	}
	if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
		return "NETWORK_PERMISSION_DENIED"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DEADLINE_EXCEEDED"
	}
	return fmt.Sprintf("error_type=%T", err)
}
