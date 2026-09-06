package wecommcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const replyTool = "message_aibot_send"
const TestReplyMarker = "TRPC-WECOM-REPLY-001"

// This is an operator-only test, not a Channel Sender or a model-accessible tool.
// The marker cannot be overridden. A prior attempt (including an unknown result)
// blocks another send, even across command restarts on this local data directory.
type replyAttempt struct {
	EndpointHash string    `json:"endpoint_hash"`
	ChatHash     string    `json:"chat_hash"`
	Marker       string    `json:"marker"`
	AttemptedAt  time.Time `json:"attempted_at"`
}

func SendUniqueTestGroupReply(ctx context.Context, endpoint, sessionsFile, outputDirectory string) (SnapshotReport, error) {
	chatID, err := probeGroup(endpoint, sessionsFile)
	if err != nil {
		return SnapshotReport{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transport := probeTransport()
	defer transport.CloseIdleConnections()
	return sendGroupReply(ctx, endpoint, &http.Client{Transport: transport, Timeout: 20 * time.Second}, chatID, outputDirectory)
}

func probeGroup(endpoint, sessionsFile string) (string, error) {
	if err := ValidateEndpoint(endpoint); err != nil {
		return "", err
	}
	snapshot, payload, err := inspectSessions(sessionsFile)
	if err != nil {
		return "", err
	}
	if snapshot.EndpointHash != endpointHash(endpoint) {
		return "", errors.New("test group snapshot is not bound to this MCP configuration")
	}
	args, err := testGroupMessageQuery(snapshot, payload, time.Now())
	if err != nil {
		return "", err
	}
	return args["chat_id"].(string), nil
}

func sendGroupReply(ctx context.Context, endpoint string, client *http.Client, chatID, outputDirectory string) (SnapshotReport, error) {
	if ctx.Err() != nil {
		return SnapshotReport{}, errors.New("test reply cancelled before attempt")
	}
	if err := claimReplyAttempt(endpoint, chatID, outputDirectory, time.Now().UTC()); err != nil {
		return SnapshotReport{}, err
	}
	args := map[string]any{"chat_id": chatID, "msg_type": "markdown", "markdown": map[string]any{"content": TestReplyMarker}}
	report, err := sampleCall(ctx, endpoint, client, replyTool, args, outputDirectory)
	if err != nil {
		// Even a response-save failure may happen after delivery. Never remove
		// the durable guard or retry automatically on any failure here.
		return SnapshotReport{}, errors.New("test reply result unavailable; do not resend; verify the same-group reply window")
	}
	return report, nil
}

func attemptPath(endpoint, chatID, directory string) string {
	key := endpointHash(endpoint + "\x00" + chatID + "\x00" + TestReplyMarker)
	return filepath.Join(directory, "wecom-mcp-reply-attempt-"+key+".json")
}

func claimReplyAttempt(endpoint, chatID, directory string, now time.Time) error {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return errors.New("cannot create private reply-attempt directory")
	}
	file, err := os.OpenFile(attemptPath(endpoint, chatID, directory), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("reply attempt already exists or cannot be recorded; no send performed")
	}
	defer func() { _ = file.Close() }()
	attempt := replyAttempt{EndpointHash: endpointHash(endpoint), ChatHash: endpointHash(chatID), Marker: TestReplyMarker, AttemptedAt: now}
	if json.NewEncoder(file).Encode(attempt) != nil || file.Sync() != nil {
		return errors.New("cannot persist reply attempt; no send performed")
	}
	parent, err := os.Open(directory)
	if err != nil {
		return errors.New("cannot open reply-attempt directory; no send performed")
	}
	defer func() { _ = parent.Close() }()
	if parent.Sync() != nil {
		return errors.New("cannot persist reply-attempt directory; no send performed")
	}
	return nil
}

// ReadUniqueTestGroupReply reads a three-minute window anchored to the recorded
// attempt, never to the group's old last-message time and never another group.
func ReadUniqueTestGroupReply(ctx context.Context, endpoint, sessionsFile, outputDirectory string) (SnapshotReport, error) {
	chatID, err := probeGroup(endpoint, sessionsFile)
	if err != nil {
		return SnapshotReport{}, err
	}
	args, err := replyWindow(endpoint, chatID, outputDirectory, time.Now())
	if err != nil {
		return SnapshotReport{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transport := probeTransport()
	defer transport.CloseIdleConnections()
	return sample(ctx, endpoint, &http.Client{Transport: transport, Timeout: 20 * time.Second}, messagesTool, args, outputDirectory)
}

func replyWindow(endpoint, chatID, directory string, now time.Time) (map[string]any, error) {
	file, err := os.Open(attemptPath(endpoint, chatID, directory))
	if err != nil {
		return nil, errors.New("no recorded reply attempt for this group")
	}
	defer func() { _ = file.Close() }()
	var attempt replyAttempt
	if json.NewDecoder(io.LimitReader(file, 4096)).Decode(&attempt) != nil || attempt.EndpointHash != endpointHash(endpoint) || attempt.ChatHash != endpointHash(chatID) || attempt.Marker != TestReplyMarker {
		return nil, errors.New("invalid reply attempt record")
	}
	if age := now.Sub(attempt.AttemptedAt); age < 0 || age > 30*time.Minute {
		return nil, errors.New("reply verification window expired")
	}
	// UTC+8 matches the observed group timestamp and the operator's local test
	// clock. This test assumption is not a claimed provider timezone contract.
	stamp := attempt.AttemptedAt.In(time.FixedZone("test-UTC+8", 8*60*60))
	return map[string]any{"chat_id": chatID, "begin_time": stamp.Add(-time.Minute).Format("2006-01-02 15:04:05"), "end_time": stamp.Add(2 * time.Minute).Format("2006-01-02 15:04:05")}, nil
}

func probeTransport() *http.Transport {
	return &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second}
}
