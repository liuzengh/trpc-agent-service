package worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	openaiopt "github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/modelendpoint"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestOperatorEndpointIsNotTenantControlled(t *testing.T) {
	t.Setenv("TRPC_OPENAI_BASE_URL", "https://operator.example/v1")
	config := tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4o-mini", APIKey: "test-key", Endpoint: "https://tenant.example/v1"}
	if _, err := NewModelFactory().CreateModel(&config); err == nil {
		t.Fatal("tenant endpoint override accepted")
	}
	config.Endpoint = ""
	t.Setenv("TRPC_OPENAI_BASE_URL", "https://username:secret@operator.example/v1")
	if _, err := NewModelFactory().CreateModel(&config); !errors.Is(err, modelendpoint.ErrConfiguration) {
		t.Fatalf("operator credentials URL accepted: %v", err)
	}
	if _, err := NewModelForTenant(context.Background(), config, nil, nil); !errors.Is(err, modelendpoint.ErrConfiguration) {
		t.Fatalf("shared summary builder skipped endpoint validation: %v", err)
	}
	if config.APIKey != "test-key" || config.Endpoint != "" {
		t.Fatal("immutable model config mutated")
	}
}

func TestOperatorModelFactoryUsesPinnedClientAndSanitizesProviderError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("SDK endpoint/key contract mismatch")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error":{"type":"secret-type","code":"secret-code","message":"leaked test-key https://private.invalid"}}`)
	}))
	defer server.Close()
	t.Setenv("TRPC_OPENAI_BASE_URL", server.URL+"/v1")
	factory := NewModelFactory()
	clientSelected := false
	factory.endpointClient = func(value string) (openaiopt.HTTPClient, error) {
		clientSelected = value == server.URL+"/v1"
		return server.Client(), nil
	}
	m, err := factory.CreateModel(&tenant.ModelConfig{Provider: "openai", ModelName: "gpt-4o-mini", APIKey: "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := m.GenerateContent(ctx, &model.Request{Messages: []model.Message{{Role: model.RoleUser, Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	sawFailure := false
	for response := range stream {
		if response != nil && response.Error != nil {
			sawFailure = true
			if response.Error.Message != "operator model request failed" || response.Error.Type != "model_provider_error" {
				t.Fatalf("unsafe provider failure: %#v", response.Error)
			}
		}
	}
	if !clientSelected || !sawFailure {
		t.Fatal("operator client or sanitized response not used")
	}
}

type endpointFakeModel struct {
	run func(context.Context) (<-chan *model.Response, error)
}

func (m endpointFakeModel) Info() model.Info { return model.Info{Name: "gpt-4o-mini"} }
func (m endpointFakeModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	return m.run(ctx)
}

func TestOperatorModelPreservesUsageAndCancelsProducer(t *testing.T) {
	producerDone := make(chan struct{})
	original := &model.Response{ID: "answer-1", Usage: &model.Usage{TotalTokens: 19}}
	m := &operatorEndpointModel{base: endpointFakeModel{run: func(ctx context.Context) (<-chan *model.Response, error) {
		out := make(chan *model.Response)
		go func() {
			defer close(out)
			defer close(producerDone)
			for {
				select {
				case out <- original:
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, nil
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	out, err := m.GenerateContent(ctx, &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-out; result != original || result.Usage.TotalTokens != 19 {
		t.Fatal("success response/usage changed")
	}
	cancel()
	for range out {
	}
	select {
	case <-producerDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled model producer leaked")
	}
}

func TestOperatorModelSanitizesImmediateFailure(t *testing.T) {
	m := &operatorEndpointModel{base: endpointFakeModel{run: func(context.Context) (<-chan *model.Response, error) {
		return nil, errors.New("test-key https://private.invalid")
	}}}
	_, err := m.GenerateContent(context.Background(), &model.Request{})
	if err == nil || strings.Contains(err.Error(), "test-key") || strings.Contains(err.Error(), "https://") {
		t.Fatalf("unsafe model failure: %v", err)
	}
}
