package wecommcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

const sessionsTool = "message_aibot_sessions_list"
const messagesTool = "chat_messages_list"

// SnapshotReport contains shapes, not conversation contents or identifiers.
type SnapshotReport struct {
	File    string            `json:"private_snapshot"`
	Tool    string            `json:"tool"`
	IsError bool              `json:"is_error"`
	Shape   any               `json:"response_shape"`
	Methods map[string]int    `json:"methods_sent"`
	Status  map[string]any    `json:"provider_status,omitempty"`
	Window  map[string]string `json:"query_window,omitempty"`
}

type privateSnapshot struct {
	Tool         string          `json:"tool"`
	CapturedAt   time.Time       `json:"captured_at"`
	IsError      bool            `json:"is_error"`
	Payload      json.RawMessage `json:"payload"`
	EndpointHash string          `json:"endpoint_hash"`
	ChatHash     string          `json:"chat_hash,omitempty"`
}

// ReadSessionSnapshot is explicitly user-authorized sampling, not discovery.
// It never calls the history, send, file or subscription interfaces.
func ReadSessionSnapshot(ctx context.Context, endpoint, outputDirectory string) (SnapshotReport, error) {
	if err := ValidateEndpoint(endpoint); err != nil {
		return SnapshotReport{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("MCP redirects disabled") }}
	return sample(ctx, endpoint, client, sessionsTool, nil, outputDirectory)
}

func sample(ctx context.Context, endpoint string, httpClient *http.Client, toolName string, args map[string]any, outputDirectory string) (SnapshotReport, error) {
	if toolName != sessionsTool && toolName != messagesTool {
		return SnapshotReport{}, errors.New("only explicitly scoped message reads may be sampled")
	}
	return sampleCall(ctx, endpoint, httpClient, toolName, args, outputDirectory)
}

// sampleCall is shared transport/response handling. Sending is reachable only
// through the fixed, explicitly authorized probe with a durable attempt guard.
func sampleCall(ctx context.Context, endpoint string, httpClient *http.Client, toolName string, args map[string]any, outputDirectory string) (SnapshotReport, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return SnapshotReport{}, errors.New("invalid sample endpoint")
	}
	if httpClient == nil {
		return SnapshotReport{}, errors.New("sample HTTP client is required")
	}
	guardClient := *httpClient
	guardClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("MCP redirects disabled") }
	guard := &discoveryHTTP{client: &guardClient, endpoint: endpoint, methods: map[string]int{}, allowedCalls: map[string]map[string]any{toolName: args}, callBudget: map[string]int{toolName: 1}}
	client, err := mcp.NewClient(endpoint, mcp.Implementation{Name: "trpc-agent-service-authorized-sample", Version: "0.1.0"}, mcp.WithClientPath(u.Path), mcp.WithClientLogger(silentLogger{}), mcp.WithClientGetSSEEnabled(false), mcp.WithHTTPReqHandler(guard))
	if err != nil {
		return SnapshotReport{}, errors.New("cannot create MCP sample client")
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Initialize(ctx, &mcp.InitializeRequest{}); err != nil {
		return SnapshotReport{}, guard.safeError("sample initialize", err)
	}
	request := &mcp.CallToolRequest{}
	request.Params.Name, request.Params.Arguments = toolName, args
	result, err := client.CallTool(ctx, request)
	if err != nil {
		return SnapshotReport{}, guard.safeError("authorized sample", err)
	}
	if result == nil {
		return SnapshotReport{}, errors.New("MCP returned no sample result")
	}
	payload, err := samplePayload(result)
	if err != nil {
		return SnapshotReport{}, err
	}
	payload = []byte(scrub(string(payload), endpoint))
	var parsed any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if decoder.Decode(&parsed) != nil {
		return SnapshotReport{}, errors.New("sample data could not be safely decoded")
	}
	// Provider identity context is neither a user message nor needed to build
	// the decoder. Do not persist it or pass it to an Agent prompt.
	if object, ok := parsed.(map[string]any); ok {
		delete(object, "extra_identity_context")
		if toolName == sessionsTool {
			if sessions, ok := object["sessions"].([]any); ok {
				for _, session := range sessions {
					if item, ok := session.(map[string]any); ok {
						delete(item, "chat_name")
					}
				}
			}
		}
		payload, _ = json.Marshal(object)
	}
	status := sampleStatus(parsed)
	if err := os.MkdirAll(outputDirectory, 0700); err != nil {
		return SnapshotReport{}, errors.New("cannot create private sample directory")
	}
	file, err := os.CreateTemp(outputDirectory, "wecom-mcp-sample-*.json")
	if err != nil {
		return SnapshotReport{}, errors.New("cannot create private sample")
	}
	defer func() { _ = file.Close() }()
	chatHash := ""
	if chat, ok := args["chat_id"].(string); ok && chat != "" {
		chatHash = endpointHash(chat)
	}
	if err := json.NewEncoder(file).Encode(privateSnapshot{Tool: toolName, CapturedAt: time.Now().UTC(), IsError: result.IsError, Payload: payload, EndpointHash: endpointHash(endpoint), ChatHash: chatHash}); err != nil {
		return SnapshotReport{}, errors.New("cannot save private sample")
	}
	if err := file.Sync(); err != nil {
		return SnapshotReport{}, errors.New("cannot flush private sample")
	}
	guard.mu.Lock()
	methods := maps.Clone(guard.methods)
	guard.mu.Unlock()
	window := map[string]string{}
	for _, key := range []string{"begin_time", "end_time"} {
		if value, ok := args[key].(string); ok {
			window[key] = value
		}
	}
	return SnapshotReport{File: filepath.Clean(file.Name()), Tool: toolName, IsError: result.IsError, Shape: payloadShape(parsed, 0), Methods: methods, Status: status, Window: window}, nil
}

func endpointHash(endpoint string) string {
	hash := sha256.Sum256([]byte(endpoint))
	return hex.EncodeToString(hash[:])
}

func sampleStatus(value any) map[string]any {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	result := map[string]any{}
	for _, key := range []string{"errcode", "sessions_count", "messages_count", "has_more", "success"} {
		switch value := object[key].(type) {
		case json.Number:
			result[key] = value
		case bool:
			result[key] = value
		}
	}
	return result
}

func InspectSampleStatus(fileName string) (map[string]any, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, errors.New("cannot open private sample")
	}
	defer func() { _ = file.Close() }()
	var snapshot privateSnapshot
	if json.NewDecoder(io.LimitReader(file, 2<<20)).Decode(&snapshot) != nil {
		return nil, errors.New("invalid private sample")
	}
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(snapshot.Payload))
	decoder.UseNumber()
	if decoder.Decode(&payload) != nil {
		return nil, errors.New("invalid private payload")
	}
	return sampleStatus(payload), nil
}

type SessionCandidate struct {
	Index           int    `json:"index"`
	ChatType        string `json:"chat_type"`
	LastMessageTime string `json:"last_message_time"`
}
type sessionListPayload struct {
	ErrorCode *int `json:"errcode"`
	Sessions  []struct {
		ChatID          string `json:"chat_id"`
		ChatType        string `json:"chat_type"`
		LastMessageTime string `json:"last_msg_time"`
	} `json:"sessions"`
}

func inspectSessions(fileName string) (privateSnapshot, sessionListPayload, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return privateSnapshot{}, sessionListPayload{}, errors.New("cannot open session snapshot")
	}
	defer func() { _ = file.Close() }()
	var snapshot privateSnapshot
	if json.NewDecoder(io.LimitReader(file, 2<<20)).Decode(&snapshot) != nil || snapshot.Tool != sessionsTool || snapshot.IsError {
		return snapshot, sessionListPayload{}, errors.New("invalid session snapshot")
	}
	var payload sessionListPayload
	if json.Unmarshal(snapshot.Payload, &payload) != nil || payload.ErrorCode == nil || *payload.ErrorCode != 0 {
		return snapshot, payload, errors.New("session list did not report success")
	}
	return snapshot, payload, nil
}

func InspectSessionCandidates(fileName string) ([]SessionCandidate, error) {
	_, payload, err := inspectSessions(fileName)
	if err != nil {
		return nil, err
	}
	result := []SessionCandidate{}
	for i, item := range payload.Sessions {
		chatType := item.ChatType
		switch chatType {
		case "single", "direct", "private", "group":
		default:
			chatType = "[unknown type]"
		}
		stamp := item.LastMessageTime
		if _, err := time.Parse("2006-01-02 15:04:05", stamp); err != nil {
			stamp = "[invalid timestamp]"
		}
		result = append(result, SessionCandidate{Index: i + 1, ChatType: chatType, LastMessageTime: stamp})
	}
	return result, nil
}

// ReadUniqueTestMessageSnapshot only samples the sole recent private session.
// It uses the provider's own time representation for a four-minute window;
// the receiver's timestamp/timezone contract still requires message evidence.
func ReadUniqueTestMessageSnapshot(ctx context.Context, endpoint, sessionsFile, outputDirectory string) (SnapshotReport, error) {
	return readTestMessageSnapshot(ctx, endpoint, sessionsFile, outputDirectory, testMessageQuery)
}

// ReadUniqueTestGroupMessageSnapshot is for an explicitly authorized, dedicated
// test group. It requires exactly one group in the recent-session snapshot; it
// never chooses the newest among multiple groups or reads private conversations.
func ReadUniqueTestGroupMessageSnapshot(ctx context.Context, endpoint, sessionsFile, outputDirectory string) (SnapshotReport, error) {
	return readTestMessageSnapshot(ctx, endpoint, sessionsFile, outputDirectory, testGroupMessageQuery)
}

func readTestMessageSnapshot(ctx context.Context, endpoint, sessionsFile, outputDirectory string, buildQuery func(privateSnapshot, sessionListPayload, time.Time) (map[string]any, error)) (SnapshotReport, error) {
	if err := ValidateEndpoint(endpoint); err != nil {
		return SnapshotReport{}, err
	}
	snapshot, payload, err := inspectSessions(sessionsFile)
	if err != nil {
		return SnapshotReport{}, err
	}
	if snapshot.EndpointHash != "" && snapshot.EndpointHash != endpointHash(endpoint) {
		return SnapshotReport{}, errors.New("sample belongs to a different MCP configuration")
	}
	if snapshot.EndpointHash == "" {
		return SnapshotReport{}, errors.New("legacy snapshot lacks endpoint binding; obtain a new authorized snapshot")
	}
	args, err := buildQuery(snapshot, payload, time.Now())
	if err != nil {
		return SnapshotReport{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second}
	defer transport.CloseIdleConnections()
	return sample(ctx, endpoint, &http.Client{Transport: transport, Timeout: 20 * time.Second}, messagesTool, args, outputDirectory)
}

func testMessageQuery(snapshot privateSnapshot, payload sessionListPayload, now time.Time) (map[string]any, error) {
	if len(payload.Sessions) != 1 {
		return nil, errors.New("test session is ambiguous; no messages were read")
	}
	item := payload.Sessions[0]
	if item.ChatType != "single" && item.ChatType != "direct" && item.ChatType != "private" {
		return nil, errors.New("sole session is not a recognized private session")
	}
	return recentMessageQuery(snapshot, item.ChatID, item.LastMessageTime, now)
}

func testGroupMessageQuery(snapshot privateSnapshot, payload sessionListPayload, now time.Time) (map[string]any, error) {
	groups := sessionListPayload{}
	for _, item := range payload.Sessions {
		if item.ChatType == "group" {
			groups.Sessions = append(groups.Sessions, item)
		}
	}
	if len(groups.Sessions) != 1 {
		return nil, errors.New("test group is ambiguous; no messages were read")
	}
	item := groups.Sessions[0]
	return recentMessageQuery(snapshot, item.ChatID, item.LastMessageTime, now)
}

func recentMessageQuery(snapshot privateSnapshot, chatID, lastMessageTime string, now time.Time) (map[string]any, error) {
	if chatID == "" || len(chatID) > 512 || strings.ContainsAny(chatID, "\x00\r\n") {
		return nil, errors.New("invalid test session identifier")
	}
	stamp, err := time.Parse("2006-01-02 15:04:05", lastMessageTime)
	if err != nil {
		return nil, errors.New("cannot scope message query to a reliable timestamp")
	}
	// Reject stale snapshots and sessions. No widening to seven-day history.
	if age := now.Sub(snapshot.CapturedAt); age < 0 || age > 30*time.Minute {
		return nil, errors.New("session sample expired")
	}
	recent := false
	for _, offset := range []time.Duration{0, 8 * time.Hour} {
		delta := snapshot.CapturedAt.Sub(stamp.Add(-offset))
		if delta >= -time.Minute && delta <= 15*time.Minute {
			recent = true
		}
	}
	if !recent {
		return nil, errors.New("session does not match the recent test window")
	}
	args := map[string]any{"chat_id": chatID, "begin_time": stamp.Add(-2 * time.Minute).Format("2006-01-02 15:04:05"), "end_time": stamp.Add(2 * time.Minute).Format("2006-01-02 15:04:05")}
	return args, nil
}

func samplePayload(result *mcp.CallToolResult) (json.RawMessage, error) {
	if result.StructuredContent != nil {
		return json.Marshal(result.StructuredContent)
	}
	if len(result.Content) == 1 {
		switch item := result.Content[0].(type) {
		case mcp.TextContent:
			if json.Valid([]byte(item.Text)) {
				return json.RawMessage(item.Text), nil
			}
		case *mcp.TextContent:
			if json.Valid([]byte(item.Text)) {
				return json.RawMessage(item.Text), nil
			}
		}
	}
	return json.Marshal(result.Content)
}

func payloadShape(value any, depth int) any {
	if depth > 12 {
		return "[depth limit]"
	}
	switch value := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for key, child := range value {
			out[key] = payloadShape(child, depth+1)
		}
		return out
	case []any:
		items := []any{}
		for i, child := range value {
			if i >= 20 {
				break
			}
			items = append(items, payloadShape(child, depth+1))
		}
		return map[string]any{"count": len(value), "items": items}
	case string:
		return fmt.Sprintf("[string: %d bytes]", len(value))
	case json.Number:
		return "[number]"
	case bool:
		return "[bool]"
	case nil:
		return nil
	default:
		return "[value]"
	}
}
