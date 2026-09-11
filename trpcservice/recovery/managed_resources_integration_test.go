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
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
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
	jobs, err := background.NewForControlPlane(repo)
	if err != nil {
		t.Fatal(err)
	}
	knowledgeRouter, err := platformstorage.NewKnowledgeRouter(repo, routed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = knowledgeRouter.Close() })
	service.WithSecretAuthorizer(routed).WithCredentialVault(vault).
		WithBackendConnections(backends).WithSkills(registry).WithSkillUploads(uploads).
		WithKnowledgeRouter(knowledgeRouter).WithBackgroundJobs(jobs).
		WithKnowledgeDocuments(admin.NewKnowledgeDocumentStore(repo)).WithAuditWriter(audit.NewMemoryWriter())
	if _, err = service.CreateTenant(ctx, controlplane.Tenant{ID: "other", DisplayName: "Other", Region: "local", SecretNamespace: "tenant/other"}); err != nil {
		t.Fatal(err)
	}
	if _, err = service.CreateAgentApp(ctx, controlplane.AgentApp{TenantID: "tutorial-tenant", ID: "managed-app", Name: "Managed"}); err != nil {
		t.Fatal(err)
	}
	if err = repo.CreateBackendBinding(ctx, controlplane.BackendBinding{ID: "managed-knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app", ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active", Config: json.RawMessage(`{"dimensions":32}`), Version: 1}); err != nil {
		t.Fatal(err)
	}
	knowledgeRevision := controlplane.DefaultBootstrapData().Revisions[0]
	knowledgeRevision.ID = "managed-knowledge-revision"
	knowledgeRevision.RevisionNo = 99
	knowledgeRevision.KnowledgeConfig = json.RawMessage(`{"enabled":true,"embedding":{"provider":"hash","dimensions":32}}`)
	knowledgeRevision.Checksum = controlplane.RevisionChecksum(knowledgeRevision)
	if err = repo.CreateRevision(ctx, knowledgeRevision); err != nil {
		t.Fatal(err)
	}
	tutorialApp, err := repo.GetAgentApp(ctx, "tutorial-tenant", "tutorial-app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.PublishRevision(ctx, "tutorial-tenant", "tutorial-app", knowledgeRevision.ID, tutorialApp.Version); err != nil {
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
	embeddingRequest := httptest.NewRequest(http.MethodPost, "/admin/knowledge/embedding-credential", bytes.NewBufferString(`{"tenant_id":"tutorial-tenant","api_key":"embedding-secret-canary"}`)).WithContext(ctx)
	embeddingRequest.Header.Set("Authorization", "Bearer "+tenantToken)
	embeddingRecorder := httptest.NewRecorder()
	handler.ServeHTTP(embeddingRecorder, embeddingRequest)
	if embeddingRecorder.Code != http.StatusCreated || strings.Contains(embeddingRecorder.Body.String(), "embedding-secret-canary") {
		t.Fatalf("embedding credential response=%d %s", embeddingRecorder.Code, embeddingRecorder.Body.String())
	}
	var embeddingResult map[string]string
	if json.Unmarshal(embeddingRecorder.Body.Bytes(), &embeddingResult) != nil || !strings.HasPrefix(embeddingResult["reference"], "managed://") {
		t.Fatal("embedding credential reference missing")
	}
	if value, err = vault.Resolve(ctx, "tutorial-tenant", secret.Embedding, embeddingResult["reference"]); err != nil || value != "embedding-secret-canary" {
		t.Fatal("embedding credential was not encrypted for the tenant", err)
	}
	if _, err = vault.Resolve(ctx, "other", secret.Embedding, embeddingResult["reference"]); !errors.Is(err, secret.ErrForbidden) {
		t.Fatal("embedding credential crossed tenant boundary")
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
	knowledgeUpload := call("/admin/knowledge/documents", tenantToken, map[string]any{"tenant_id": "tutorial-tenant", "app_id": "tutorial-app", "revision_id": knowledgeRevision.ID, "document_id": "managed-handbook", "operation_id": "managed-handbook-v1", "name": "Managed handbook", "content": "the managed handbook retention period is forty days", "metadata": map[string]string{"category": "policy"}}, 202)
	knowledgeJobID := decodeID(knowledgeUpload, "job_id")
	job, err := jobs.Claim(ctx, "knowledge-test-worker", time.Minute)
	if err != nil || job.ID != knowledgeJobID || job.Type != background.JobKnowledgeUpsert {
		t.Fatalf("knowledge upload job=%+v err=%v", job, err)
	}
	var knowledgePayload background.KnowledgeUpsertPayload
	if json.Unmarshal(job.Payload, &knowledgePayload) != nil {
		t.Fatal("invalid knowledge upload payload")
	}
	knowledgeScope, _ := runtimecontext.NewScope(job.TenantID, job.AppID, job.RevisionID, "test", "knowledge")
	if _, err = knowledgeRouter.UpsertDocument(runtimecontext.WithStorageScope(ctx, knowledgeScope.StorageScope), knowledgeScope, knowledgeRevision, knowledgePayload.Document); err != nil {
		t.Fatal(err)
	}
	if err = jobs.Complete(ctx, job.ID, "knowledge-test-worker"); err != nil {
		t.Fatal(err)
	}
	documentList := call("/admin/knowledge/documents/list", tenantToken, map[string]string{"tenant_id": "tutorial-tenant", "app_id": "tutorial-app"}, 200)
	if !strings.Contains(string(documentList["items"]), `"document_id":"managed-handbook"`) || !strings.Contains(string(documentList["items"]), `"status":"ready"`) || strings.Contains(string(documentList["items"]), "forty days") {
		t.Fatal("knowledge document catalog is incomplete or leaked content")
	}
	replayed := call("/admin/knowledge/documents", tenantToken, map[string]any{"tenant_id": "tutorial-tenant", "app_id": "tutorial-app", "revision_id": knowledgeRevision.ID, "document_id": "managed-handbook", "operation_id": "managed-handbook-v1", "name": "Managed handbook", "content": "the managed handbook retention period is forty days", "metadata": map[string]string{"category": "policy"}}, 202)
	if string(replayed["duplicate"]) != "true" {
		t.Fatal("knowledge upload retry was not idempotent")
	}
	documentList = call("/admin/knowledge/documents/list", tenantToken, map[string]string{"tenant_id": "tutorial-tenant", "app_id": "tutorial-app"}, 200)
	if !strings.Contains(string(documentList["items"]), `"status":"ready"`) {
		t.Fatal("completed upload retry regressed document state")
	}
	otherDocuments := call("/admin/knowledge/documents/list", otherToken, map[string]string{"tenant_id": "other"}, 200)
	if string(otherDocuments["items"]) != "[]" {
		t.Fatal("knowledge document catalog crossed tenant boundary")
	}
	deleteResult := call("/admin/knowledge/documents/delete", tenantToken, map[string]string{"tenant_id": "tutorial-tenant", "app_id": "tutorial-app", "revision_id": knowledgeRevision.ID, "document_id": "managed-handbook", "operation_id": "managed-handbook-delete"}, 202)
	deleteJobID := decodeID(deleteResult, "job_id")
	job, err = jobs.Claim(ctx, "knowledge-test-worker", time.Minute)
	if err != nil || job.ID != deleteJobID || job.Type != background.JobKnowledgeDelete {
		t.Fatalf("knowledge delete job=%+v err=%v", job, err)
	}
	if err = knowledgeRouter.DeleteDocument(runtimecontext.WithStorageScope(ctx, knowledgeScope.StorageScope), knowledgeScope, knowledgeRevision, "managed-handbook"); err != nil {
		t.Fatal(err)
	}
	if err = jobs.Complete(ctx, job.ID, "knowledge-test-worker"); err != nil {
		t.Fatal(err)
	}
	documentList = call("/admin/knowledge/documents/list", tenantToken, map[string]string{"tenant_id": "tutorial-tenant", "app_id": "tutorial-app"}, 200)
	if string(documentList["items"]) != "[]" {
		t.Fatal("completed knowledge deletion remained in active catalog")
	}
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
	if value, err = restoredVault.Resolve(ctx, "tutorial-tenant", secret.Embedding, embeddingResult["reference"]); err != nil || value != "embedding-secret-canary" {
		t.Fatal("restored embedding credential unavailable", err)
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
