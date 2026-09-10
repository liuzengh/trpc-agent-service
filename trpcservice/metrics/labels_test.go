package metrics

import "testing"

func TestMetricLabelsExcludeRequestAndIdentityCardinality(t *testing.T) {
	attributes := (Labels{
		TenantID:  "tenant-a",
		AppID:     "app-a",
		Channel:   "wecom",
		Provider:  "openai",
		Operation: "request",
		Result:    "success",
		ErrorType: "",
	}).attributes()
	for _, item := range attributes {
		switch string(item.Key) {
		case "user_id", "session_id", "request_id", "trace_id", "message_id", "artifact_id":
			t.Fatalf("forbidden high-cardinality metric label %q", item.Key)
		}
	}
}
