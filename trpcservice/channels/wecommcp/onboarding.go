package wecommcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

const onboardingMarker = "TRPC-WECOM-TEST-001"

// Onboarding uses a previously confirmed group fingerprint, not "the newest
// group". The operator supplies the already-authorized sample timestamp; the
// window stays at four minutes even if the latest group activity has changed.
func ReadConfirmedGroupSample(ctx context.Context, endpoint, sessionsFile, attemptFile, sampleTime, outputDir string) (SnapshotReport, error) {
	chat, err := confirmedGroup(endpoint, sessionsFile, attemptFile, time.Now())
	if err != nil {
		return SnapshotReport{}, err
	}
	stamp, err := time.ParseInLocation("2006-01-02 15:04:05", sampleTime, time.FixedZone("UTC+8", 8*60*60))
	if err != nil || stamp.After(time.Now()) || stamp.Before(time.Now().Add(-7*24*time.Hour+2*time.Minute)) {
		return SnapshotReport{}, errors.New("invalid or expired onboarding sample time")
	}
	transport := probeTransport()
	defer transport.CloseIdleConnections()
	return sample(ctx, endpoint, &http.Client{Transport: transport, Timeout: 20 * time.Second}, messagesTool, map[string]any{"chat_id": chat, "begin_time": stamp.Add(-2 * time.Minute).Format("2006-01-02 15:04:05"), "end_time": stamp.Add(2 * time.Minute).Format("2006-01-02 15:04:05")}, outputDir)
}

func confirmedGroup(endpoint, sessionsFile, attemptFile string, now time.Time) (string, error) {
	if err := ValidateEndpoint(endpoint); err != nil {
		return "", err
	}
	snapshot, payload, err := inspectSessions(sessionsFile)
	if err != nil {
		return "", err
	}
	if snapshot.EndpointHash != endpointHash(endpoint) || now.Before(snapshot.CapturedAt) || now.Sub(snapshot.CapturedAt) > time.Hour {
		return "", errors.New("onboarding requires a fresh endpoint-bound session snapshot")
	}
	file, err := os.Open(attemptFile)
	if err != nil {
		return "", errors.New("cannot open confirmed group fingerprint")
	}
	defer func() { _ = file.Close() }()
	var attempt replyAttempt
	if json.NewDecoder(io.LimitReader(file, 4096)).Decode(&attempt) != nil || attempt.EndpointHash != snapshot.EndpointHash || attempt.Marker != TestReplyMarker || attempt.AttemptedAt.After(now) || now.Sub(attempt.AttemptedAt) > 7*24*time.Hour {
		return "", errors.New("invalid confirmed group record")
	}
	match := ""
	count := 0
	for _, item := range payload.Sessions {
		if item.ChatType == "group" && validIDs([]string{item.ChatID}, 1) && endpointHash(item.ChatID) == attempt.ChatHash {
			match = item.ChatID
			count++
		}
	}
	if count != 1 {
		return "", errors.New("confirmed test group not uniquely present; no other group was read")
	}
	return match, nil
}

// BuildConfirmedGroupBinding consumes only local, scope-bound samples. It does
// not save, enable or send anything. Admin authorization remains separate.
func BuildConfirmedGroupBinding(endpoint, sessionsFile, messagesFile, attemptFile string, binding controlplane.ChannelBinding, start time.Time) (controlplane.ChannelBinding, error) {
	chat, err := confirmedGroup(endpoint, sessionsFile, attemptFile, time.Now())
	if err != nil {
		return binding, err
	}
	file, err := os.Open(messagesFile)
	if err != nil {
		return binding, errors.New("cannot open onboarding message sample")
	}
	defer func() { _ = file.Close() }()
	var snapshot privateSnapshot
	if json.NewDecoder(io.LimitReader(file, 2<<20)).Decode(&snapshot) != nil || snapshot.Tool != messagesTool || snapshot.IsError || snapshot.EndpointHash != endpointHash(endpoint) || snapshot.ChatHash != endpointHash(chat) {
		return binding, errors.New("message sample does not belong to the confirmed group")
	}
	var payload struct {
		ErrorCode *int `json:"errcode"`
		Messages  []struct {
			UserID string `json:"userid"`
			Type   string `json:"msg_type"`
			Text   struct {
				Content string `json:"content"`
			} `json:"text"`
		} `json:"messages"`
	}
	if json.Unmarshal(snapshot.Payload, &payload) != nil || payload.ErrorCode == nil || *payload.ErrorCode != 0 {
		return binding, errors.New("onboarding message query was not successful")
	}
	user, prefix, style := "", "", "whitespace"
	for _, m := range payload.Messages {
		if m.Type != "text" || !strings.HasSuffix(strings.TrimSpace(m.Text.Content), onboardingMarker) {
			continue
		}
		rawPrefix := strings.TrimSuffix(strings.TrimSpace(m.Text.Content), onboardingMarker)
		mention := strings.TrimSpace(rawPrefix)
		if !validIDs([]string{m.UserID}, 1) || !strings.HasPrefix(mention, "@") {
			continue
		}
		if user != "" && (user != m.UserID || prefix != mention) {
			return binding, errors.New("onboarding marker has ambiguous sender or mention")
		}
		user, prefix = m.UserID, mention
		if rawPrefix == mention {
			style = "prefix"
		}
	}
	if user == "" {
		return binding, errors.New("confirmed marker or human identity missing")
	}
	binding.ChannelType = ChannelType
	binding.Status = controlplane.StatusActive
	cfg := BindingConfig{AllowedChatIDs: []string{chat}, AllowedUserIDs: []string{user}, MentionPrefix: prefix, MentionStyle: style, Timezone: "Asia/Shanghai", StartAt: start.UTC().Truncate(time.Second).Format(time.RFC3339), DedupeMode: "fingerprint-v1"}
	binding.Config, _ = json.Marshal(cfg)
	if _, err := ParseBinding(binding); err != nil {
		return binding, err
	}
	return binding, nil
}
