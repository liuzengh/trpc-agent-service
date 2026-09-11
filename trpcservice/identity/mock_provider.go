package identity

import (
	"context"
	"errors"
	"net/url"
	"strings"
)

// MockConfig configures the explicit development/test login provider.
// Production composition only enables this provider when LOGIN_MOCK_ENABLED=true.
type MockConfig struct {
	ProviderID      string
	DisplayName     string
	SubjectID       string
	UserDisplayName string
	Email           string
}

type MockProvider struct{ config MockConfig }

func NewMockProvider(config MockConfig) (*MockProvider, error) {
	config.ProviderID = strings.TrimSpace(config.ProviderID)
	config.DisplayName = strings.TrimSpace(config.DisplayName)
	config.SubjectID = strings.TrimSpace(config.SubjectID)
	config.UserDisplayName = strings.TrimSpace(config.UserDisplayName)
	config.Email = strings.TrimSpace(config.Email)
	if config.ProviderID == "" || config.SubjectID == "" {
		return nil, errors.New("mock provider requires provider_id and subject_id")
	}
	if config.DisplayName == "" {
		config.DisplayName = "Mock 测试登录"
	}
	if config.UserDisplayName == "" {
		config.UserDisplayName = config.SubjectID
	}
	return &MockProvider{config: config}, nil
}

func (p *MockProvider) Descriptor() ProviderDescriptor {
	return ProviderDescriptor{ProviderID: p.config.ProviderID, Type: ProviderMock, DisplayName: p.config.DisplayName}
}

func (p *MockProvider) ConfigurationMetadata() map[string]string {
	return map[string]string{"subject_id": p.config.SubjectID}
}

func (p *MockProvider) Begin(request AuthRequest) (string, error) {
	if strings.TrimSpace(request.State) == "" {
		return "", errors.New("mock provider requires a state")
	}
	query := url.Values{}
	query.Set("code", "mock")
	query.Set("state", request.State)
	return "/api/v1/auth/callback?" + query.Encode(), nil
}

func (p *MockProvider) Exchange(_ context.Context, exchange AuthExchange) (Identity, error) {
	if strings.TrimSpace(exchange.Code) != "mock" {
		return Identity{}, errors.New("mock provider received an invalid callback code")
	}
	return Identity{
		ProviderID: p.config.ProviderID, ProviderType: ProviderMock,
		EnterpriseID: "mock:" + p.config.ProviderID, SubjectID: p.config.SubjectID,
		DisplayName: p.config.UserDisplayName, Email: p.config.Email,
	}, nil
}

var _ IdentityProvider = (*MockProvider)(nil)
var _ IdentityProviderConfiguration = (*MockProvider)(nil)
