package message

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"strings"
	"time"
)

const (
	TaskSchemaVersion = 2
	maxTaskIDBytes    = 512
	maxTaskTextBytes  = 16 << 10
)

type ExecutionTask struct {
	SchemaVersion     int              `json:"schema_version"`
	TaskID            string           `json:"task_id"`
	Channel           string           `json:"channel"`
	ChannelBindingID  string           `json:"channel_binding_id"`
	ExternalAccountID string           `json:"external_account_id"`
	TenantID          string           `json:"tenant_id"`
	AgentAppID        string           `json:"agent_app_id"`
	ConfigVersion     string           `json:"config_version"`
	RunnerUserID      string           `json:"runner_user_id"`
	SessionID         string           `json:"session_id"`
	PlatformMessageID string           `json:"platform_message_id"`
	ActorUserID       string           `json:"actor_user_id"`
	ConversationID    string           `json:"conversation_id"`
	ConversationType  ConversationType `json:"conversation_type"`
	ReplyToMessageID  string           `json:"reply_to_message_id,omitempty"`
	PlatformRequestID string           `json:"platform_request_id,omitempty"`
	Text              string           `json:"text"`
	RequestID         string           `json:"request_id"`
	TraceID           string           `json:"trace_id"`
	TraceParent       string           `json:"trace_parent,omitempty"`
	DigestVersion     int              `json:"digest_version,omitempty"`
	ReceivedAt        time.Time        `json:"received_at"`
	Attempt           int              `json:"attempt"`
	PayloadDigest     string           `json:"payload_digest"`
}

type TaskResult struct {
	SchemaVersion int             `json:"schema_version"`
	TaskID        string          `json:"task_id"`
	Succeeded     bool            `json:"succeeded"`
	Channel       string          `json:"channel,omitempty"`    // Legacy projection of Target.
	BindingID     string          `json:"binding_id,omitempty"` // Legacy projection of Target.
	Target        DeliveryTarget  `json:"target,omitempty"`
	Reply         OutboundMessage `json:"reply,omitempty"`
	ErrorCode     string          `json:"error_code,omitempty"`
	TraceID       string          `json:"trace_id"`
	TraceParent   string          `json:"trace_parent,omitempty"`
	DigestVersion int             `json:"digest_version,omitempty"`
}

func (r TaskResult) Validate() error {
	if r.SchemaVersion != TaskSchemaVersion || r.TaskID == "" || len(r.TaskID) > maxTaskIDBytes || r.TraceID == "" || len(r.TraceID) > maxTaskIDBytes {
		return errors.New("task result metadata is invalid")
	}
	if r.Succeeded {
		if r.Channel == "" || r.BindingID == "" || strings.TrimSpace(r.Reply.Text) == "" {
			return errors.New("successful task result is incomplete")
		}
	} else if r.ErrorCode == "" {
		return errors.New("failed task result error_code is missing")
	}
	if r.Target != (DeliveryTarget{}) {
		if !r.Target.Valid() {
			return errors.New("task result delivery target is invalid")
		}
		if r.Channel != r.Target.Channel || r.BindingID != r.Target.ChannelBindingID {
			return errors.New("task result delivery target does not match projection")
		}
	}
	return nil
}

func (t ExecutionTask) InboxID() string {
	return digestParts(t.TenantID, t.ChannelBindingID, t.PlatformMessageID)
}

func (t ExecutionTask) DeliveryTarget() DeliveryTarget {
	return DeliveryTarget{
		Channel: t.Channel, ChannelBindingID: t.ChannelBindingID, ExternalAccountID: t.ExternalAccountID,
		ConversationID: t.ConversationID, ConversationType: t.ConversationType,
		ReplyToMessageID: t.ReplyToMessageID, PlatformRequestID: t.PlatformRequestID,
	}
}

func (t ExecutionTask) CanonicalDigest() string {
	parts := t.canonicalDigestParts()
	if t.DigestVersion >= 2 && t.TraceParent != "" {
		parts = append([]string{"phase6-digest-v2", t.TraceParent}, parts...)
	}
	return digestParts(parts...)
}

// BusinessDigest identifies the executable message payload without transport
// trace metadata. Trace parents may legitimately differ across client retries;
// they must not turn one platform message into a payload conflict.
func (t ExecutionTask) BusinessDigest() string {
	t.TraceParent = ""
	t.DigestVersion = 0
	return t.CanonicalDigest()
}

func (t ExecutionTask) canonicalDigestParts() []string {
	return []string{
		t.Channel,
		t.ChannelBindingID,
		t.ExternalAccountID,
		t.PlatformMessageID,
		t.TenantID,
		t.AgentAppID,
		t.ConfigVersion,
		t.RunnerUserID,
		t.SessionID,
		t.ActorUserID,
		t.ConversationID,
		string(t.ConversationType),
		t.ReplyToMessageID,
		t.PlatformRequestID,
		t.Text,
	}
}

func (t ExecutionTask) ValidDigest() bool {
	return ConstantTimeDigestEqual(t.PayloadDigest, t.CanonicalDigest())
}

func ConstantTimeDigestEqual(left, right string) bool {
	leftBytes, err := hex.DecodeString(left)
	if err != nil {
		return false
	}
	rightBytes, err := hex.DecodeString(right)
	if err != nil || len(leftBytes) != len(rightBytes) {
		return false
	}
	return subtle.ConstantTimeCompare(leftBytes, rightBytes) == 1
}

func (t ExecutionTask) Validate() error {
	if t.SchemaVersion != TaskSchemaVersion {
		return errors.New("unsupported task schema_version")
	}
	for _, value := range []string{
		t.TaskID, t.Channel, t.ChannelBindingID, t.ExternalAccountID, t.TenantID, t.AgentAppID,
		t.ConfigVersion, t.RunnerUserID, t.SessionID, t.PlatformMessageID,
		t.ActorUserID, t.ConversationID, t.RequestID, t.TraceID,
	} {
		if value == "" || len(value) > maxTaskIDBytes {
			return errors.New("task contains a missing or oversized identifier")
		}
	}
	if strings.TrimSpace(t.Text) == "" || len(t.Text) > maxTaskTextBytes {
		return errors.New("task text is missing or oversized")
	}
	if !t.ConversationType.Valid() {
		return errors.New("task conversation_type is invalid")
	}
	if t.ReceivedAt.IsZero() || t.Attempt < 1 {
		return errors.New("task metadata is invalid")
	}
	if !t.ValidDigest() {
		return errors.New("task payload digest is invalid")
	}
	return nil
}

func digestParts(parts ...string) string {
	digest := sha256.New()
	for _, part := range parts {
		writePart(digest, part)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func writePart(destination hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = destination.Write(size[:])
	_, _ = destination.Write([]byte(value))
}
