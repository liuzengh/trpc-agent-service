package sessiondriver

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const sessionWatermarkPrefix = "session-key-v1:"

type sessionWatermark struct {
	AgentAppID string `json:"agent_app_id"`
	SessionID  string `json:"session_id"`
	Empty      bool   `json:"empty,omitempty"`
}

func EncodeSessionWatermark(agentAppID, sessionID string, empty bool) (string, error) {
	if !empty && (agentAppID == "" || sessionID == "") {
		return "", runtime.ErrInvariantViolation
	}
	data, err := json.Marshal(sessionWatermark{AgentAppID: agentAppID, SessionID: sessionID, Empty: empty})
	if err != nil {
		return "", err
	}
	return sessionWatermarkPrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

func DecodeSessionWatermark(value string) (agentAppID, sessionID string, empty bool, err error) {
	if !strings.HasPrefix(value, sessionWatermarkPrefix) {
		return "", "", false, runtime.ErrInvariantViolation
	}
	data, decodeErr := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, sessionWatermarkPrefix))
	if decodeErr != nil {
		return "", "", false, runtime.ErrInvariantViolation
	}
	var cursor sessionWatermark
	if json.Unmarshal(data, &cursor) != nil || (!cursor.Empty && (cursor.AgentAppID == "" || cursor.SessionID == "")) {
		return "", "", false, runtime.ErrInvariantViolation
	}
	return cursor.AgentAppID, cursor.SessionID, cursor.Empty, nil
}
