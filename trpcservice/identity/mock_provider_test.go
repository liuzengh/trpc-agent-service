package identity

import (
	"context"
	"net/url"
	"testing"
)

func TestMockProviderUsesSameAuthTransactionFlow(t *testing.T) {
	provider, err := NewMockProvider(MockConfig{
		ProviderID: "mock", SubjectID: "tester", UserDisplayName: "测试用户", Email: "tester@example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	authURL, err := provider.Begin(AuthRequest{State: "signed-state"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "/api/v1/auth/callback" || parsed.Query().Get("state") != "signed-state" || parsed.Query().Get("code") != "mock" {
		t.Fatalf("mock auth URL = %q", authURL)
	}
	got, err := provider.Exchange(context.Background(), AuthExchange{Code: "mock"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ProviderType != ProviderMock || got.EnterpriseID != "mock:mock" || got.SubjectID != "tester" || got.DisplayName != "测试用户" {
		t.Fatalf("mock identity = %+v", got)
	}
}

func TestMockProviderRejectsInvalidCallback(t *testing.T) {
	provider, err := NewMockProvider(MockConfig{ProviderID: "mock", SubjectID: "tester"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Exchange(context.Background(), AuthExchange{Code: "other"}); err == nil {
		t.Fatal("invalid mock callback must fail")
	}
}
