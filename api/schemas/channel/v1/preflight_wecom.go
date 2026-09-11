package channelv1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/gowebpki/jcs"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	PreflightWeComPolicy = "wecom_long_connection_v1"
	PreflightWeComMode   = "long_connection"
	PreflightWeComURL    = "wss://openws.work.weixin.qq.com"
)

// CredentialVersion selects the provider-specific immutable diagnostic input.
func (v PreflightView) CredentialVersion() int64 {
	if v.Provider == "wecom" {
		return v.BotSecretVersion
	}
	return v.BotTokenVersion
}

// PreflightConfigDigestForPolicy preserves Telegram's existing digest bytes.
// WeCom authenticates through a real subscription, not a read-only identity API.
func PreflightConfigDigestForPolicy(policy, scope, epoch string, origin *string, status string) (string, error) {
	if policy == "" || policy == PreflightReceiveModesPolicy {
		return PreflightConfigDigest(scope, epoch, origin, status)
	}
	if policy != PreflightWeComPolicy || !preflightID.MatchString(scope) || !preflightEpoch.MatchString(epoch) || origin != nil || status != "PUBLIC_ORIGIN_NOT_APPLICABLE" {
		return "", ErrInvalidDocument
	}
	return preflightWeComDigest(map[string]any{
		"schema_version": 1, "diagnostic_policy": policy, "scope_id": scope,
		"source_epoch": epoch, "wecom_ws_endpoint": PreflightWeComURL,
	})
}

func preflightWeComEffectiveDigest(scope, epoch string, revision int64, origin *string, status string) (string, error) {
	if _, err := PreflightConfigDigestForPolicy(PreflightWeComPolicy, scope, epoch, origin, status); err != nil || revision < 1 || revision > 9007199254740991 {
		return "", ErrInvalidDocument
	}
	return preflightWeComDigest(map[string]any{
		"diagnostic_policy": PreflightWeComPolicy, "scope_id": scope,
		"source_epoch": epoch, "receive_mode": PreflightWeComMode,
		"connection_revision": revision, "wecom_ws_endpoint": PreflightWeComURL,
	})
}

func preflightWeComDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", ErrInvalidDocument
	}
	raw, err = jcs.Transform(raw)
	if err != nil {
		return "", ErrInvalidDocument
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateWeComPreflightChecks(mode string, checks []PreflightCheck) (string, error) {
	if mode != PreflightWeComMode {
		return "", ErrInvalidDocument
	}
	compiled.Do(compile)
	if compiled.err != nil {
		return "", ErrUnknownSchema
	}
	raw, err := json.Marshal(struct {
		Mode   string           `json:"receive_mode"`
		Checks []PreflightCheck `json:"checks"`
	}{mode, checks})
	if err != nil {
		return "", ErrInvalidDocument
	}
	if _, err = jcs.Transform(raw); err != nil {
		return "", ErrInvalidDocument
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil || compiled.schemas["preflight-checks.schema.json"].Validate(value) != nil {
		return "", ErrInvalidDocument
	}
	if (checks[0].Code == "BOT_SECRET_MISSING") != (checks[1].Code == "NOT_EXECUTED") {
		return "", ErrInvalidDocument
	}
	if checks[0].Status == "FAIL" || checks[1].Status == "FAIL" {
		return "FAIL", nil
	}
	if checks[1].Status == "PASS" {
		return "PASS", nil
	}
	return "UNKNOWN", nil
}
