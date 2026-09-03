package runtimecontext

import "testing"

func TestNewScope(t *testing.T) {
	scope, err := NewScope("tenant-a", "app-a", "revision-1", "http", "binding-1")
	if err != nil {
		t.Fatalf("new scope: %v", err)
	}
	if scope.StorageScope != "t/tenant-a/a/app-a" {
		t.Fatalf("storage scope = %q", scope.StorageScope)
	}
	if err := scope.Validate(); err != nil {
		t.Fatalf("validate scope: %v", err)
	}
}

func TestScopeRejectsInvalidOrForgedIdentifiers(t *testing.T) {
	if _, err := NewScope("../tenant", "app", "revision", "http", "binding"); err == nil {
		t.Fatal("expected invalid tenant error")
	}
	scope := TutorialScope()
	scope.StorageScope = "t/other/a/tutorial-app"
	if err := scope.Validate(); err == nil {
		t.Fatal("expected forged storage scope error")
	}
}

func TestParseStorageScope(t *testing.T) {
	tenantID, appID, err := ParseStorageScope("t/tenant-a/a/app-a")
	if err != nil || tenantID != "tenant-a" || appID != "app-a" {
		t.Fatalf("tenant=%q app=%q err=%v", tenantID, appID, err)
	}
	if _, _, err := ParseStorageScope("tenant-a/app-a"); err == nil {
		t.Fatal("expected forged scope error")
	}
}
