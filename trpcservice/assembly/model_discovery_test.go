package assembly

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDiscoverOpenAIModelsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-key" {
			t.Errorf("unexpected auth header: %s", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"object": "list",
			"data": [
				{"id": "glm-5.3-flash", "object": "model"},
				{"id": "glm-4-plus", "object": "model"},
				{"id": "glm-5.3-flash", "object": "model"}
			]
		}`))
	}))
	defer server.Close()

	models, err := DiscoverOpenAIModels(context.Background(), server.Client(), server.URL+"/v1", "test-key")
	if err != nil {
		t.Fatalf("DiscoverOpenAIModels failed: %v", err)
	}

	if len(models) != 2 {
		t.Fatalf("expected 2 unique models, got %d: %v", len(models), models)
	}
	if models[0] != "glm-4-plus" || models[1] != "glm-5.3-flash" {
		t.Errorf("unexpected models order/content: %v", models)
	}
}

func TestDiscoverOpenAIModelsErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"invalid_api_key"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := DiscoverOpenAIModels(context.Background(), server.Client(), server.URL, "bad-key")
	if err == nil {
		t.Fatal("expected error on 401, got nil")
	}
}
