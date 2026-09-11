package assembly

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type backendHealthProbeModel struct {
	name     string
	response *model.Response
	err      error
}

func (m *backendHealthProbeModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	responses := make(chan *model.Response, 1)
	responses <- m.response
	close(responses)
	return responses, nil
}

func (m *backendHealthProbeModel) Info() model.Info { return model.Info{Name: m.name} }

func TestObservedModelOpensCircuitOnTransientResponseFailure(t *testing.T) {
	registry, err := backendhealth.NewRegistry(backendhealth.Config{
		ConsecutiveFailures: 1, MinimumSamples: 1, WindowSize: 1, FailureRatio: 1,
		InitialOpen: time.Minute, MaxOpen: time.Minute, HalfOpenSuccesses: 1, HalfOpenMaxRequests: 1,
		ProbeInterval: time.Minute, ProbeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	code := "429"
	delegate := &backendHealthProbeModel{name: "primary", response: &model.Response{
		Done: true, Model: "primary", Error: &model.ResponseError{Message: "quota exceeded", Code: &code},
	}}
	observed := observeModel(delegate, registry, "primary-provider", "openai")
	responses, err := observed.GenerateContent(context.Background(), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}
	key := backendhealth.Key{ProfileID: "primary-provider", Domain: modelBackendDomain, Driver: "openai"}
	if status := registry.Status(key); status.State != backendhealth.StateOpen {
		t.Fatalf("model circuit state = %q, want open", status.State)
	}
	if _, err := observed.GenerateContent(context.Background(), &model.Request{}); !errors.Is(err, backendhealth.ErrCircuitOpen) {
		t.Fatalf("GenerateContent(open circuit) error = %v, want ErrCircuitOpen", err)
	}
}

func TestObservedModelIgnoresDeterministicProviderRejectionForCircuit(t *testing.T) {
	registry, err := backendhealth.NewRegistry(backendhealth.Config{
		ConsecutiveFailures: 1, MinimumSamples: 1, WindowSize: 1, FailureRatio: 1,
		InitialOpen: time.Minute, MaxOpen: time.Minute, HalfOpenSuccesses: 1, HalfOpenMaxRequests: 1,
		ProbeInterval: time.Minute, ProbeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	delegate := &backendHealthProbeModel{name: "primary", response: &model.Response{
		Done: true, Model: "primary", Error: &model.ResponseError{Message: "invalid request", Type: "invalid_request_error"},
	}}
	observed := observeModel(delegate, registry, "primary-provider", "openai")
	responses, err := observed.GenerateContent(context.Background(), &model.Request{})
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}
	key := backendhealth.Key{ProfileID: "primary-provider", Domain: modelBackendDomain, Driver: "openai"}
	if status := registry.Status(key); status.State != backendhealth.StateHealthy {
		t.Fatalf("model circuit state = %q, want healthy for deterministic rejection", status.State)
	}
}
