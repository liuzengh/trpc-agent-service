package admin

import (
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

func TestAdminBrowserIntegration(t *testing.T) {
	if os.Getenv("TEST_ADMIN_UI_BROWSER") != "1" {
		t.Skip("explicit Playwright browser suite disabled")
	}
	module := os.Getenv("TEST_PLAYWRIGHT_MODULE")
	if module == "" {
		t.Fatal("TEST_PLAYWRIGHT_MODULE must name an installed Playwright package")
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sample"), 0700); err != nil {
		t.Fatal(err)
	}
	for path, raw := range map[string]string{"catalog.json": `[{"name":"sample","version":"1","directory":"sample"}]`, "sample/SKILL.md": "---\nname: sample\ndescription: Browser fixture skill\n---\nUse the isolated runner.", "sample/run.sh": "echo fixture"} {
		if err := os.WriteFile(filepath.Join(root, path), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := platformskill.Load(root, `[{"tenant_id":"browser-tenant","name":"sample","version":"1"}]`)
	if err != nil {
		t.Fatal(err)
	}
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	defer func() { _ = repo.Close() }()
	writer := audit.NewMemoryWriter()
	defer func() { _ = writer.Close() }()
	skills := &platformskill.Service{Registry: registry}
	service, err := New(repo, platformtool.DefaultCatalog(skills.RunTool()))
	if err != nil {
		t.Fatal(err)
	}
	service.WithAuditWriter(writer)
	service.WithSkills(registry)
	const token = "browser-fixture-admin-token-123456789012345"
	h, err := NewHandlerWithPrincipals(service, []Principal{{Name: "browser-admin", Role: RoleSuperAdmin, Token: token}, {Name: "browser-auditor", Role: RoleAuditor, Token: "browser-fixture-auditor-token-123456789", TenantIDs: []string{"browser-tenant"}}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "ui/browser.test.cjs")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "TEST_PLAYWRIGHT_MODULE=" + module, "TEST_BROWSER_EXECUTABLE=" + os.Getenv("TEST_BROWSER_EXECUTABLE"), "TEST_ADMIN_UI_URL=" + server.URL, "TEST_ADMIN_UI_TOKEN=" + token, "TEST_ADMIN_UI_SCREENSHOT=" + os.Getenv("TEST_ADMIN_UI_SCREENSHOT")}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("browser verification failed: %v\n%s", err, output)
	} else {
		t.Log(string(output))
	}
}
