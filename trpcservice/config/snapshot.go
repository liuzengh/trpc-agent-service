package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

const CurrentSchemaVersion uint16 = 1

type ChannelBinding struct {
	BindingID         string            `json:"binding_id"`
	Channel           string            `json:"channel"`
	ExternalAccountID string            `json:"external_account_id"`
	AgentAppID        string            `json:"agent_app_id"`
	SecretRef         secrets.SecretRef `json:"secret_ref"`
	SendSecretRef     secrets.SecretRef `json:"send_secret_ref,omitempty"`
}

func RequiresSendSecret(channel string) bool { return channel == "feishu" || channel == "wecom" }

func ValidSendSecret(binding ChannelBinding) bool {
	return !RequiresSendSecret(binding.Channel) || (binding.SendSecretRef.Ref != "" && binding.SendSecretRef.Version >= 1)
}

type BackendBinding struct {
	Domain           string   `json:"domain"`
	BackendProfileID string   `json:"backend_profile_id"`
	BackendVersion   int64    `json:"backend_version"`
	Required         []string `json:"required,omitempty"`
}

type ConfigV1 struct {
	SchemaVersion     uint16           `json:"schema_version"`
	DefaultAgentAppID string           `json:"default_agent_app_id"`
	PolicyVersion     int64            `json:"policy_version"`
	ChannelBindings   []ChannelBinding `json:"channel_bindings,omitempty"`
	BackendBindings   []BackendBinding `json:"backend_bindings,omitempty"`
}

type SnapshotState string

const (
	StateStaged    SnapshotState = "staged"
	StatePublished SnapshotState = "published"
)

type Snapshot struct {
	TenantID      string
	ConfigVersion int64
	SchemaVersion uint16
	Payload       ConfigV1
	ContentDigest string
	State         SnapshotState
	PublishedAt   time.Time
	CreatedAt     time.Time
}

func DecodeV1(data []byte) (ConfigV1, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var value ConfigV1
	if err := dec.Decode(&value); err != nil {
		return ConfigV1{}, fmt.Errorf("decode config: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return ConfigV1{}, fmt.Errorf("decode config trailing data")
	}
	return value, nil
}

func NormalizeV1(in ConfigV1) ConfigV1 {
	out := in
	out.ChannelBindings = append([]ChannelBinding(nil), in.ChannelBindings...)
	out.BackendBindings = append([]BackendBinding(nil), in.BackendBindings...)
	for i := range out.BackendBindings {
		out.BackendBindings[i].Required = append([]string(nil), in.BackendBindings[i].Required...)
		sort.Strings(out.BackendBindings[i].Required)
	}
	sort.Slice(out.ChannelBindings, func(i, j int) bool { return out.ChannelBindings[i].BindingID < out.ChannelBindings[j].BindingID })
	sort.Slice(out.BackendBindings, func(i, j int) bool { return out.BackendBindings[i].Domain < out.BackendBindings[j].Domain })
	return out
}

func ContentDigest(in ConfigV1) (string, []byte, error) {
	data, err := json.Marshal(NormalizeV1(in))
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), data, nil
}
