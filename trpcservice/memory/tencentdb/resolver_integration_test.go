//go:build integration && external

package tencentdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory/tencentdb"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
)

func TestRealGatewayCaptureAndSearch(t *testing.T) {
	gatewayURL := strings.TrimRight(strings.TrimSpace(os.Getenv("TRPC_AGENT_SERVICE_TENCENTDB_GATEWAY_URL")), "/")
	apiKey := strings.TrimSpace(os.Getenv("TRPC_AGENT_SERVICE_TENCENTDB_API_KEY"))
	if gatewayURL == "" || apiKey == "" {
		t.Skip("set TRPC_AGENT_SERVICE_TENCENTDB_GATEWAY_URL and TRPC_AGENT_SERVICE_TENCENTDB_API_KEY")
	}

	resolver, err := NewResolver(
		&recordingSecrets{value: apiKey},
		&recordingGateways{url: gatewayURL},
	)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exec := testExecution("integration-user")
	ingestor, err := resolver.ResolveSessionIngestor(ctx, exec)
	if err != nil {
		t.Fatalf("resolve real gateway ingestor: %v", err)
	}
	service, ok := ingestor.(*frameworkmemory.Service)
	if !ok {
		t.Fatalf("real gateway ingestor type = %T", ingestor)
	}
	health, err := service.Health(ctx)
	if err != nil {
		t.Fatalf("real gateway health: %v", err)
	}
	if health == nil || health.Status != "ok" || !health.Stores.VectorStore {
		t.Fatalf("real gateway health = %+v", health)
	}

	marker := fmt.Sprintf("trpc-agent-service local gateway integration %d", time.Now().UnixNano())
	sessionID := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	sess := frameworksession.NewSession(
		"app-a",
		"integration-user",
		sessionID,
		frameworksession.WithSessionEvents([]event.Event{
			*event.NewResponseEvent(
				"integration-user-turn",
				"user",
				&model.Response{Choices: []model.Choice{{Message: model.NewUserMessage(marker)}}},
			),
			*event.NewResponseEvent(
				"integration-assistant-turn",
				"assistant",
				&model.Response{
					Done:    true,
					Choices: []model.Choice{{Message: model.NewAssistantMessage("local gateway capture completed")}},
				},
			),
		}),
	)
	if err := service.IngestSession(ctx, sess); err != nil {
		t.Fatalf("ingest session through resolver service: %v", err)
	}
	if err := service.EndSession(ctx, sess); err != nil {
		t.Fatalf("end session after real capture: %v", err)
	}

	sessionKey := "tenant:tenant-a:app:app-a:memory:integration-user:" + sessionID
	requestBody, err := json.Marshal(map[string]any{
		"query":       marker,
		"limit":       5,
		"session_key": sessionKey,
	})
	if err != nil {
		t.Fatalf("marshal conversation search: %v", err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		gatewayURL+"/search/conversations",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		t.Fatalf("build conversation search: %v", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+apiKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("search conversations through real gateway: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatalf("read conversation search: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("conversation search status = %d body = %s", response.StatusCode, body)
	}
	var searchResult struct {
		Results string `json:"results"`
		Total   int    `json:"total"`
	}
	if err := json.Unmarshal(body, &searchResult); err != nil {
		t.Fatalf("decode conversation search: %v", err)
	}
	if searchResult.Total < 1 || !strings.Contains(searchResult.Results, marker) {
		t.Fatalf("conversation search result = %+v", searchResult)
	}
}
