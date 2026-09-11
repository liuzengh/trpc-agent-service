package storage

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestPostgresArtifactScopeValidationAndUserScope(t *testing.T) {
	t.Parallel()
	info := agentartifact.SessionInfo{AppName: "tenant-a/support", UserID: "user-1", SessionID: "session-1"}
	scope, err := newArtifactScope(info, "report.txt")
	if err != nil {
		t.Fatal(err)
	}
	if scope.tenantID != "tenant-a" || scope.appCode != "support" || scope.userID != "user-1" || scope.sessionID != "session-1" || scope.filename != "report.txt" {
		t.Fatalf("session scope = %#v", scope)
	}
	userScope, err := newArtifactScope(info, "user:profile.json")
	if err != nil {
		t.Fatal(err)
	}
	if userScope.sessionID != "" || userScope.filename != "user:profile.json" {
		t.Fatalf("user scope = %#v", userScope)
	}

	for _, test := range []struct {
		name     string
		info     agentartifact.SessionInfo
		filename string
	}{
		{name: "missing app", info: agentartifact.SessionInfo{UserID: "u", SessionID: "s"}, filename: "a.txt"},
		{name: "bad tenant", info: agentartifact.SessionInfo{AppName: "../support", UserID: "u", SessionID: "s"}, filename: "a.txt"},
		{name: "extra app path", info: agentartifact.SessionInfo{AppName: "tenant/support/extra", UserID: "u", SessionID: "s"}, filename: "a.txt"},
		{name: "missing user", info: agentartifact.SessionInfo{AppName: "tenant/support", SessionID: "s"}, filename: "a.txt"},
		{name: "missing session", info: agentartifact.SessionInfo{AppName: "tenant/support", UserID: "u"}, filename: "a.txt"},
		{name: "missing filename", info: agentartifact.SessionInfo{AppName: "tenant/support", UserID: "u", SessionID: "s"}},
		{name: "nul filename", info: agentartifact.SessionInfo{AppName: "tenant/support", UserID: "u", SessionID: "s"}, filename: "bad\x00name"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newArtifactScope(test.info, test.filename); err == nil {
				t.Fatal("newArtifactScope() error = nil")
			}
		})
	}
}

func TestPostgresArtifactValidationRejectsInvalidContent(t *testing.T) {
	t.Parallel()
	if err := validateFrameworkArtifact(nil); err == nil {
		t.Fatal("nil artifact accepted")
	}
	if err := validateFrameworkArtifact(&agentartifact.Artifact{MimeType: "text/plain"}); err == nil {
		t.Fatal("nil data accepted")
	}
	if err := validateFrameworkArtifact(&agentartifact.Artifact{Data: []byte("x")}); err == nil {
		t.Fatal("missing mime type accepted")
	}
	if err := validateFrameworkArtifact(&agentartifact.Artifact{Data: make([]byte, MaxArtifactBytes+1), MimeType: "application/octet-stream"}); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("oversized artifact error = %v", err)
	}
	if err := validateFrameworkArtifact(&agentartifact.Artifact{Data: []byte("ok"), MimeType: "text/plain"}); err != nil {
		t.Fatalf("valid artifact error = %v", err)
	}
}

func TestPostgresArtifactCRUDValidatesBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresArtifactService(nil); err == nil {
		t.Fatal("NewPostgresArtifactService(nil) error = nil")
	}
	database, err := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	service, err := NewPostgresArtifactService(database)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	badInfo := agentartifact.SessionInfo{AppName: "bad", UserID: "user-1", SessionID: "session-1"}
	if _, err := service.SaveArtifact(ctx, badInfo, "report.txt", &agentartifact.Artifact{Data: []byte("x"), MimeType: "text/plain"}); err == nil {
		t.Fatal("SaveArtifact() reached database with invalid scope")
	}
	if _, err := service.LoadArtifact(ctx, badInfo, "report.txt", nil); err == nil {
		t.Fatal("LoadArtifact() reached database with invalid scope")
	}
	if _, err := service.ListArtifactKeys(ctx, badInfo); err == nil {
		t.Fatal("ListArtifactKeys() reached database with invalid scope")
	}
	if err := service.DeleteArtifact(ctx, badInfo, "report.txt"); err == nil {
		t.Fatal("DeleteArtifact() reached database with invalid scope")
	}
	if _, err := service.ListVersions(ctx, badInfo, "report.txt"); err == nil {
		t.Fatal("ListVersions() reached database with invalid scope")
	}

	goodInfo := agentartifact.SessionInfo{AppName: "tenant-a/support", UserID: "user-1", SessionID: "session-1"}
	negative := -1
	if _, err := service.LoadArtifact(ctx, goodInfo, "report.txt", &negative); err == nil || !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("negative version error = %v", err)
	}
	if _, err := service.SaveArtifact(ctx, goodInfo, "report.txt", nil); err == nil {
		t.Fatal("SaveArtifact(nil) reached database")
	}
}
