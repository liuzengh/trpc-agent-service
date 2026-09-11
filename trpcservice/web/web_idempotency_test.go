package web

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestConsoleChatRequestIDPreventsDuplicateKafkaPublish(t *testing.T) {
	producer := &recordingProducer{}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) { dependencies.Producer = producer })
	body := `{"tenant_id":"example","app_code":"support","conversation_id":"conv-1","text":"hello","request_id":"11111111-2222-4333-8444-555555555555"}`
	var firstID string
	for attempt := 0; attempt < 2; attempt++ {
		recorder := handler.request(t, http.MethodPost, "/api/v1/chat", body)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("attempt %d status = %d: %s", attempt, recorder.Code, recorder.Body.String())
		}
		var response struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if attempt == 0 {
			firstID = response.EventID
		} else if response.EventID != firstID {
			t.Fatalf("duplicate event ID = %q, want %q", response.EventID, firstID)
		}
	}
	if len(producer.published) != 1 {
		t.Fatalf("Kafka publishes = %d, want 1", len(producer.published))
	}
}
