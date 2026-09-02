//go:build integration

package skill

import (
	"context"
	"errors"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const skillSchema = `CREATE TABLE IF NOT EXISTS skills (
    skill_id        VARCHAR(36)  NOT NULL,
    scope           ENUM('global','tenant') NOT NULL DEFAULT 'tenant',
    owner_tenant_id VARCHAR(36)  NULL,
    code            VARCHAR(64)  NOT NULL,
    name            VARCHAR(128) NOT NULL,
    description     VARCHAR(512) NULL,
    current_version INT          NOT NULL DEFAULT 0,
    status          ENUM('draft','published','disabled') NOT NULL DEFAULT 'draft',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (skill_id),
    UNIQUE KEY uk_skill_code (code),
    KEY idx_skill_scope_tenant (scope, owner_tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS skill_versions (
    skill_id        VARCHAR(36)  NOT NULL,
    version         INT          NOT NULL,
    content_md      MEDIUMTEXT   NOT NULL,
    checksum        VARCHAR(64)  NOT NULL,
    prompt_template MEDIUMTEXT   NULL,
    executor_type   VARCHAR(32)  NOT NULL DEFAULT 'inline',
    timeout_seconds INT          NOT NULL DEFAULT 30,
    status          ENUM('draft','published','disabled') NOT NULL DEFAULT 'draft',
    published_at    DATETIME     NULL,
    PRIMARY KEY (skill_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_skills (
    agent_id   VARCHAR(36) NOT NULL,
    skill_id   VARCHAR(36) NOT NULL,
    version    INT         NOT NULL,
    sort_order INT         NOT NULL DEFAULT 0,
    created_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agent_id, skill_id),
    KEY idx_agent_skills_skill (skill_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`

func TestMySQLSkillLifecycle(t *testing.T) {
	ctx := context.Background()
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(skillSchema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	m := NewMySQLManager(db)

	// Create -> defaults
	acme := "acme"
	s := &Skill{Code: "mysql-skill", Name: "MySQL Skill", Scope: ScopeTenant, OwnerTenantID: &acme}
	if err := m.Create(ctx, s); err != nil {
		t.Fatalf("create: %v", err)
	}
	if s.SkillID == "" || s.Status != StatusDraft || s.CurrentVersion != 0 {
		t.Errorf("defaults: %+v", s)
	}

	// Duplicate code rejected by the unique index.
	dup := &Skill{Code: "mysql-skill", Name: "Dup", Scope: ScopeTenant, OwnerTenantID: &acme}
	if err := m.Create(ctx, dup); !errors.Is(err, ErrSkillCodeExists) {
		t.Errorf("dup code = %v, want ErrSkillCodeExists", err)
	}

	// Version round-trip + publish switches the pointer.
	if err := m.CreateVersion(ctx, &SkillVersion{SkillID: s.SkillID, Version: 1, ContentMD: "# SKILL\nfirst"}); err != nil {
		t.Fatalf("create version: %v", err)
	}
	if err := m.PublishVersion(ctx, s.SkillID, 1); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cur, err := m.Get(ctx, s.SkillID)
	if err != nil {
		t.Fatal(err)
	}
	if cur.CurrentVersion != 1 || cur.Status != StatusPublished {
		t.Errorf("after publish: current=%d status=%s", cur.CurrentVersion, cur.Status)
	}
	v1, err := m.GetVersion(ctx, s.SkillID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Checksum == "" || v1.Status != StatusPublished || v1.PublishedAt.IsZero() {
		t.Errorf("version after publish: checksum=%q status=%s", v1.Checksum, v1.Status)
	}

	// Bind + load for an agent, sorted by sort_order.
	if err := m.BindAgentSkill(ctx, "a-1", s.SkillID, 1, 0); err != nil {
		t.Fatalf("bind: %v", err)
	}
	loaded, err := m.LoadForAgent(ctx, "a-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Code != "mysql-skill" || loaded[0].ContentMD != "# SKILL\nfirst" {
		t.Errorf("loaded = %+v", loaded)
	}

	// Tenant filtering: acme sees own + global.
	if err := m.Create(ctx, &Skill{Code: "g-skill", Name: "G", Scope: ScopeGlobal}); err != nil {
		t.Fatal(err)
	}
	acmeList, err := m.List(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if len(acmeList) != 2 {
		t.Errorf("acme sees %d skills, want 2 (own + global)", len(acmeList))
	}

	// Delete unbinds agents and hides the skill.
	if err := m.Delete(ctx, s.SkillID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := m.Get(ctx, s.SkillID); !errors.Is(err, ErrSkillNotFound) {
		t.Errorf("after delete = %v, want not found", err)
	}
	loadedAfter, _ := m.LoadForAgent(ctx, "a-1")
	if len(loadedAfter) != 0 {
		t.Errorf("agent still has skill after delete: %+v", loadedAfter)
	}
}
