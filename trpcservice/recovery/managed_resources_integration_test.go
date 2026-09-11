package recovery_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/backendregistry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

func TestIsolatedManagedResources(t *testing.T) {
	if os.Getenv("TEST_RECOVERY_DOCKER") != "1" {
		t.Skip("isolated Docker checks disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cid, addr := isolatedPostgres(t, ctx)
	db, err := sql.Open("pgx", "postgres://drill@"+addr+"/source?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			t.Fatal("isolated database unavailable")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err = database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = controlplane.SeedBootstrap(ctx, db, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	repo, err := controlplane.NewPostgresRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	master := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{37}, 32))
	vault, err := credentials.New(repo, master)
	if err != nil {
		t.Fatal(err)
	}
	env, _ := secret.NewEnvStore(nil)
	routed := credentials.Routed{Vault: vault, Fallback: env, Allowed: credentials.Purposes}
	backends := backendregistry.New(repo, vault, routed)
	uploads := platformskill.NewStore(repo)
	registry, err := platformskill.Load("", "")
	if err != nil {
		t.Fatal(err)
	}
	registry.WithManagedStore(uploads)
	skillService := &platformskill.Service{Registry: registry, Repository: repo}
	service, err := admin.New(repo, platformtool.DefaultCatalog(skillService.RunTool()))
	if err != nil {
		t.Fatal(err)
	}
	service.WithSecretAuthorizer(routed).WithBackendConnections(backends).WithSkills(registry).WithSkillUploads(uploads).WithAuditWriter(audit.NewMemoryWriter())
	if _, err = service.CreateTenant(ctx, controlplane.Tenant{ID: "other", DisplayName: "Other", Region: "local", SecretNamespace: "tenant/other"}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.CreateAgentApp(ctx, controlplane.AgentApp{TenantID: "tutorial-tenant", ID: "managed-app", Name: "Managed"}); err != nil {
		t.Fatal(err)
	}
	const rootToken = "synthetic-root-token-for-resources-123456"
	const tenantToken = "synthetic-tenant-token-for-resources-1234"
	const otherToken = "synthetic-other-token-for-resources-12345"
	const readerToken = "synthetic-reader-token-for-resources-1234"
	handler, err := admin.NewHandlerWithPrincipals(service, []admin.Principal{
		{Name: "root", Role: admin.RoleSuperAdmin, Token: rootToken},
		{Name: "tenant", Role: admin.RoleTenantAdmin, Token: tenantToken, TenantIDs: []string{"tutorial-tenant"}},
		{Name: "other", Role: admin.RoleTenantAdmin, Token: otherToken, TenantIDs: []string{"other"}},
		{Name: "reader", Role: admin.RoleAuditor, Token: readerToken, TenantIDs: []string{"tutorial-tenant"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	call := func(path, token string, body any, want int) map[string]json.RawMessage {
		t.Helper()
		raw, e := json.Marshal(body)
		if e != nil {
			t.Fatal(e)
		}
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)).WithContext(ctx)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s status=%d want=%d response=%s", path, w.Code, want, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "connection-secret-canary") || strings.Contains(w.Body.String(), "managed://") {
			t.Fatal("connection secret or internal reference leaked")
		}
		var result map[string]json.RawMessage
		if e = json.Unmarshal(w.Body.Bytes(), &result); e != nil {
			t.Fatal(e)
		}
		return result
	}
	decodeID := func(result map[string]json.RawMessage, key string) string {
		t.Helper()
		var id string
		if json.Unmarshal(result[key], &id) != nil || id == "" {
			t.Fatal("missing response identifier")
		}
		return id
	}
	request := map[string]any{"tenant_id": "tutorial-tenant", "name": "Team Redis", "resource_type": "session", "backend_type": "redis", "settings": map[string]any{"host": "redis.example.invalid", "port": 6379, "database": "0"}, "credentials": map[string]string{"password": "connection-secret-canary"}}
	request["connection_id"] = "storage-fixture"
	call("/admin/backend-connections/create", tenantToken, request, 403)
	call("/admin/backend-connections/create", readerToken, request, 403)
	id := decodeID(call("/admin/backend-connections/create", rootToken, request, 201), "connection_id")
	call("/admin/backend-connections/create", rootToken, request, 409)
	var credentialCount int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM channel_credential WHERE tenant_id='tutorial-tenant'`).Scan(&credentialCount); err != nil || credentialCount != 1 {
		t.Fatal("duplicate creation left an orphan credential", err)
	}
	c, err := backends.Get(ctx, "tutorial-tenant", id)
	if err != nil {
		t.Fatal(err)
	}
	value, err := vault.Resolve(ctx, "tutorial-tenant", secret.Session, c.SecretRef)
	if err != nil || !strings.Contains(value, "connection-secret-canary") {
		t.Fatal("encrypted credential did not resolve")
	}
	if _, err = vault.Resolve(ctx, "other", secret.Session, c.SecretRef); !errors.Is(err, secret.ErrForbidden) {
		t.Fatal("cross-tenant credential accepted")
	}
	var ciphertext []byte
	if err = db.QueryRowContext(ctx, `SELECT ciphertext FROM channel_credential WHERE tenant_id=$1 AND reference=$2`, c.TenantID, c.SecretRef).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, []byte("connection-secret-canary")) {
		t.Fatal("credential stored in plaintext")
	}
	list := call("/admin/backend-connections/list", otherToken, map[string]string{"tenant_id": "other"}, 200)
	if string(list["items"]) != "[]" {
		t.Fatal("connection list crossed tenant")
	}
	call("/admin/backend-connections/bind", otherToken, map[string]string{"tenant_id": "other", "connection_id": id}, 404)
	bind := map[string]string{"tenant_id": "tutorial-tenant", "connection_id": id, "app_id": "managed-app"}
	call("/admin/backend-connections/bind", tenantToken, bind, 201)
	call("/admin/backend-connections/bind", tenantToken, bind, 409)
	// Managed credentials cannot be redirected through the old JSON binding API.
	call("/admin/backend-bindings", tenantToken, map[string]any{"tenant_id": "tutorial-tenant", "resource_type": "session", "backend_type": "redis", "secret_ref": c.SecretRef, "config": map[string]string{"url": "redis://attacker.example.invalid"}}, 403)
	call("/admin/backend-bindings", tenantToken, map[string]any{"tenant_id": "tutorial-tenant", "resource_type": "session", "backend_type": "redis", "config": map[string]string{"url": "redis://attacker.example.invalid"}}, 403)
	call("/admin/backend-connections/create", tenantToken, map[string]any{"tenant_id": "tutorial-tenant", "name": "Memory", "resource_type": "memory", "backend_type": "inmemory", "settings": map[string]any{}, "credentials": map[string]any{}}, 201)
	md := "---\nname: uploaded\ndescription: Uploaded resource test\n---\nRead input and return a bounded answer.\n"
	upload := map[string]any{"tenant_id": "tutorial-tenant", "name": "uploaded", "version": "1", "markdown": md, "script": "#!/bin/sh\nprintf 'sandbox-only\\n'\n"}
	call("/admin/skills/upload", readerToken, upload, 403)
	call("/admin/skills/upload", tenantToken, upload, 200)
	available := call("/admin/skills/list", tenantToken, map[string]string{"tenant_id": "tutorial-tenant"}, 200)
	if string(available["items"]) != "[]" {
		t.Fatal("unapproved upload became available")
	}
	call("/admin/skills/inspect", otherToken, map[string]string{"tenant_id": "other", "name": "uploaded", "version": "1"}, 404)
	review := map[string]any{"tenant_id": "tutorial-tenant", "name": "uploaded", "version": "1", "status": "approved", "expected_revision": 1}
	call("/admin/skills/review", tenantToken, review, 403)
	call("/admin/skills/review", rootToken, review, 200)
	call("/admin/skills/review", rootToken, review, 409)
	otherRegistry, _ := platformskill.Load("", "")
	otherRegistry.WithManagedStore(platformskill.NewStore(repo))
	items, err := otherRegistry.ListContext(ctx, "tutorial-tenant")
	if err != nil || len(items) != 1 {
		t.Fatal("another node cannot see approved upload", err)
	}
	ref := items[0].Ref
	if _, err = otherRegistry.RepositoryForContext(ctx, "other", []platformskill.Ref{ref}); err == nil {
		t.Fatal("cross-tenant uploaded Skill accepted")
	}
	upload["script"] = "echo different"
	call("/admin/skills/upload", tenantToken, upload, 409)
	if _, err = db.ExecContext(ctx, `UPDATE skill_bundle SET script='modified' WHERE tenant_id='tutorial-tenant' AND name='uploaded'`); err == nil {
		t.Fatal("database allowed mutation of Skill content")
	}
	revision := controlplane.DefaultBootstrapData().Revisions[0]
	revision.ID = "managed-skill-revision"
	revision.RevisionNo = 2
	revision.AgentConfig, _ = json.Marshal(map[string]any{"name": "managed", "instruction": "Use approved skill instructions.", "skills": []platformskill.Ref{ref}})
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load"],"max_tool_calls":4}`)
	revision.Checksum = controlplane.RevisionChecksum(revision)
	if err = repo.CreateRevision(ctx, revision); err != nil {
		t.Fatal(err)
	}
	compiler, err := agentruntime.NewRevisionCompiler(repo, agentruntime.NewTutorialModel(), false, agentruntime.WithToolCatalog(platformtool.DefaultCatalog()), agentruntime.WithSkills(otherRegistry))
	if err != nil {
		t.Fatal(err)
	}
	scope := runtimecontext.TutorialScope()
	scope.RevisionID = revision.ID
	if _, err = compiler.Compile(ctx, scope); err != nil {
		t.Fatal("approved Skill cannot compile", err)
	}
	call("/admin/skills/review", rootToken, map[string]any{"tenant_id": "tutorial-tenant", "name": "uploaded", "version": "1", "status": "revoked", "expected_revision": 2}, 200)
	if _, err = compiler.Compile(ctx, scope); err == nil {
		t.Fatal("cached Agent ignored revoked Skill approval")
	}
	if _, err = otherRegistry.RepositoryForContext(ctx, "tutorial-tenant", []platformskill.Ref{ref}); err == nil {
		t.Fatal("runtime snapshot ignored revocation")
	}
	items, err = otherRegistry.ListContext(ctx, "tutorial-tenant")
	if err != nil || len(items) != 0 {
		t.Fatal("revoked Skill remained available")
	}
	// Persisted uploads and encryption keys must survive a real database restore.
	docker(t, ctx, "exec", cid, "pg_dump", "-U", "drill", "-Fc", "-f", "/tmp/resources.dump", "source")
	docker(t, ctx, "exec", cid, "createdb", "-U", "drill", "resources_restored")
	docker(t, ctx, "exec", cid, "pg_restore", "--exit-on-error", "--single-transaction", "--no-owner", "-U", "drill", "-d", "resources_restored", "/tmp/resources.dump")
	restored, err := sql.Open("pgx", "postgres://drill@"+addr+"/resources_restored?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	restoredRepo, err := controlplane.NewPostgresRepository(restored)
	if err != nil {
		t.Fatal(err)
	}
	restoredVault, err := credentials.New(restoredRepo, master)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restoredVault.Resolve(ctx, "tutorial-tenant", secret.Session, c.SecretRef); err != nil {
		t.Fatal("restored connection credential unavailable", err)
	}
	d, mdRestored, _, err := platformskill.NewStore(restoredRepo).Inspect(ctx, "tutorial-tenant", "uploaded", "1")
	if err != nil || d.Status != "revoked" || mdRestored != md || d.Checksum != ref.Checksum {
		t.Fatal("restored Skill state or content changed", err)
	}
	if _, err = service.CreateAgentApp(ctx, controlplane.AgentApp{TenantID: "tutorial-tenant", ID: "browser-app", Name: "Browser App"}); err != nil {
		t.Fatal(err)
	}
	exerciseResourceBrowser(t, ctx, handler, rootToken)
}
