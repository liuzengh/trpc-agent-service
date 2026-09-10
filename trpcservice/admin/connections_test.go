package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

//nolint:gocyclo // Covers the complete HTTP route validation and forwarding contract.
func TestAdminConnectionsRoutesValidateAndForwardRequests(t *testing.T) {
	if _, _, err := (&Handler{}).connections(httptest.NewRequest(http.MethodGet, "/", nil), Principal{}, "tenant", nil); !errors.Is(err, ErrConnectionUnavailable) {
		t.Fatalf("missing connection service error = %v", err)
	}

	service := &adminConnectionsStub{list: []Connection{{BindingID: "binding", Channel: channels.ChannelTelegram, BotID: "123", Ready: true}}}
	handler := &Handler{config: Config{Connections: service}}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	status, value, err := handler.connections(request, Principal{SubjectID: "operator"}, "tenant-a", nil)
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET connections = status:%d value:%#v err:%v", status, value, err)
	}
	if got := value.([]Connection); len(got) != 1 || got[0].BindingID != "binding" || service.listTenant != "tenant-a" {
		t.Fatalf("GET connections value = %+v, service = %+v", got, service)
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"channel":"telegram","bot_id":"123","secret":"runtime-secret"}`))
	request.Header.Set("X-Request-ID", "request-123")
	status, value, err = handler.connections(request, Principal{SubjectID: "operator"}, "tenant-a", nil)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("POST connections = status:%d value:%#v err:%v", status, value, err)
	}
	if service.input.Secret != "runtime-secret" || service.metadata.ActorID != "operator" || service.metadata.CorrelationID != "request-123" {
		t.Fatalf("POST forwarding = input:%+v metadata:%+v", service.input, service.metadata)
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"channel":"telegram","bot_id":"123","secret":"runtime-secret"}`))
	service.input = ConnectInput{}
	service.metadata = channels.ChangeMetadata{}
	if _, _, err := handler.connections(request, Principal{SubjectID: "operator"}, "tenant-a", nil); err != nil {
		t.Fatal(err)
	}
	if service.metadata.CorrelationID != "web-channel" {
		t.Fatalf("default correlation ID = %q", service.metadata.CorrelationID)
	}

	for name, body := range map[string]string{
		"malformed":      `{`,
		"unknown field":  `{"channel":"telegram","bot_id":"123","secret":"secret","unknown":true}`,
		"trailing value": `{"channel":"telegram","bot_id":"123","secret":"secret"}{}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			if _, _, err := handler.connections(request, Principal{}, "tenant-a", nil); !errors.Is(err, errInvalidRequest) {
				t.Fatalf("invalid POST error = %v", err)
			}
		})
	}

	request = httptest.NewRequest(http.MethodDelete, "/", nil)
	status, value, err = handler.connections(request, Principal{SubjectID: "operator"}, "tenant-a", []string{"binding"})
	if err != nil || status != http.StatusOK || !value.(map[string]bool)["disconnected"] || service.bindingID != "binding" {
		t.Fatalf("DELETE connections = status:%d value:%#v err:%v service:%+v", status, value, err, service)
	}
	if _, _, err := handler.connections(httptest.NewRequest(http.MethodGet, "/", nil), Principal{}, "tenant-a", []string{"binding"}); !errors.Is(err, errNotFound) {
		t.Fatalf("unsupported connection route error = %v", err)
	}
}

func TestAdminConnectionsPropagateServiceErrors(t *testing.T) {
	listErr := errors.New("list unavailable")
	connectErr := errors.New("connect unavailable")
	disconnectErr := errors.New("disconnect unavailable")
	service := &adminConnectionsStub{listErr: listErr, connectErr: connectErr, disconnectErr: disconnectErr}
	handler := &Handler{config: Config{Connections: service}}
	if status, _, err := handler.connections(httptest.NewRequest(http.MethodGet, "/", nil), Principal{}, "tenant", nil); status != http.StatusOK || !errors.Is(err, listErr) {
		t.Fatalf("list error = status:%d err:%v", status, err)
	}
	if status, _, err := handler.connections(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"channel":"telegram","bot_id":"123","secret":"secret"}`)), Principal{}, "tenant", nil); status != http.StatusCreated || !errors.Is(err, connectErr) {
		t.Fatalf("connect error = status:%d err:%v", status, err)
	}
	status, value, err := handler.connections(httptest.NewRequest(http.MethodDelete, "/", nil), Principal{}, "tenant", []string{"binding"})
	if status != http.StatusOK || !errors.Is(err, disconnectErr) || value.(map[string]bool)["disconnected"] {
		t.Fatalf("disconnect error = status:%d value:%#v err:%v", status, value, err)
	}
}

func TestCacheInvalidatorFuncForwardsOnlyNonNilFunctions(t *testing.T) {
	called := false
	CacheInvalidatorFunc(func(change CacheInvalidation) {
		called = change.TenantID == "tenant" && change.Kind == CacheInvalidationTenant
	}).Invalidate(CacheInvalidation{TenantID: "tenant", Kind: CacheInvalidationTenant})
	if !called {
		t.Fatal("CacheInvalidatorFunc did not forward the invalidation")
	}
	var nilInvalidator CacheInvalidatorFunc
	nilInvalidator.Invalidate(CacheInvalidation{TenantID: "tenant"})
}

type adminConnectionsStub struct {
	list          []Connection
	listErr       error
	connectErr    error
	disconnectErr error
	listTenant    string
	input         ConnectInput
	metadata      channels.ChangeMetadata
	bindingID     string
}

func (service *adminConnectionsStub) Connect(_ context.Context, _ string, input ConnectInput, metadata channels.ChangeMetadata) (Connection, error) {
	service.input = input
	service.metadata = metadata
	if service.connectErr != nil {
		return Connection{}, service.connectErr
	}
	return Connection{BindingID: "created", Channel: input.Channel, BotID: input.BotID}, nil
}

func (service *adminConnectionsStub) List(_ context.Context, tenantID string) ([]Connection, error) {
	service.listTenant = tenantID
	if service.listErr != nil {
		return nil, service.listErr
	}
	return service.list, nil
}

func (service *adminConnectionsStub) Disconnect(_ context.Context, _ string, bindingID string, _ channels.ChangeMetadata) error {
	service.bindingID = bindingID
	return service.disconnectErr
}

var _ ChannelConnections = (*adminConnectionsStub)(nil)
