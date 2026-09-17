package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type demoModelStub struct{}

func (demoModelStub) ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error) {
	return demoResponseModel{}, nil
}

type demoResponseModel struct{}

func (demoResponseModel) GenerateContent(_ context.Context, _ *model.Request) (<-chan *model.Response, error) {
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{ID: "demo-response", Model: "fake-deterministic-v1", Done: true,
		Choices: []model.Choice{{Message: model.NewAssistantMessage("fixed response")}}}
	close(responses)
	return responses, nil
}

func (demoResponseModel) Info() model.Info { return model.Info{Name: "fake-deterministic-v1"} }

type demoStreamingModel struct{}

func (demoStreamingModel) GenerateContent(_ context.Context, request *model.Request) (<-chan *model.Response, error) {
	if request == nil || !request.GenerationConfig.Stream {
		return nil, context.Canceled
	}
	responses := make(chan *model.Response, 2)
	responses <- &model.Response{ID: "demo-stream", Model: "fake-deterministic-v1",
		Choices: []model.Choice{{Delta: model.Message{Role: model.RoleAssistant, Content: "first "}}}}
	responses <- &model.Response{ID: "demo-stream", Model: "fake-deterministic-v1", Done: true,
		Choices: []model.Choice{{Delta: model.Message{Role: model.RoleAssistant, Content: "second"}}}}
	close(responses)
	return responses, nil
}

func (demoStreamingModel) Info() model.Info { return model.Info{Name: "fake-deterministic-v1"} }

type demoStreamingResolver struct{}

func (demoStreamingResolver) ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error) {
	return demoStreamingModel{}, nil
}

func TestDemoHTTPHandlerServesHealthReadinessAndFakeChat(t *testing.T) {
	handler := newDemoHTTPHandler(demoModelStub{}, func(context.Context) error { return nil })
	for _, path := range []string{"/healthz", "/readyz"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"message":"same input"}`)))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"response":"fixed response"`) {
		t.Fatalf("chat status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDemoHTTPHandlerRejectsInvalidChat(t *testing.T) {
	handler := newDemoHTTPHandler(demoModelStub{}, func(context.Context) error { return nil })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"unexpected":true}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestDemoHTTPHandlerStreamsModelDeltasAsSSE(t *testing.T) {
	handler := newDemoHTTPHandler(demoStreamingResolver{}, func(context.Context) error { return nil })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"message":"same input","stream":true}`)))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}
	body := response.Body.String()
	for _, expected := range []string{
		`event: delta`, `"id":"demo-stream"`, `"delta":"first "`, `"done":false`,
		`"delta":"second"`, `"done":true`, "event: done\ndata: {}",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("stream body missing %q: %s", expected, body)
		}
	}
}
