package channelv1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/gowebpki/jcs"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const PreflightReceiveModesPolicy = "telegram-receive-modes-v1"

// PreflightEffectiveConfigDigest binds mode and connection identity without
// allowing an unrelated inbound origin to invalidate long-polling diagnostics.
// Account/task identity and lease authorization are separately fixed by grant.
func PreflightEffectiveConfigDigest(scope, epoch, mode string, connectionRevision int64, origin *string, originStatus string, endpoint ...string) (string, error) {
	apiOrigin := "https://api.telegram.org"
	if len(endpoint) > 1 {
		return "", ErrInvalidDocument
	}
	if len(endpoint) == 1 {
		switch endpoint[0] {
		case "", "official":
		case "test":
			if mode == PreflightWeComMode {
				return "", ErrInvalidDocument
			}
			apiOrigin = "http://channel-lab:8080"
		default:
			return "", ErrInvalidDocument
		}
	}
	if mode == PreflightWeComMode {
		return preflightWeComEffectiveDigest(scope, epoch, connectionRevision, origin, originStatus)
	}
	if connectionRevision < 1 || connectionRevision > 9007199254740991 || (mode != "long_polling" && mode != "webhook") {
		return "", ErrInvalidDocument
	}
	if _, err := PreflightConfigDigest(scope, epoch, origin, originStatus); err != nil {
		return "", err
	}
	if mode == "long_polling" {
		origin = nil
		originStatus = "PUBLIC_ORIGIN_NOT_APPLICABLE"
	}
	raw, err := json.Marshal(struct {
		Policy            string  `json:"diagnostic_policy"`
		Scope             string  `json:"scope_id"`
		Epoch             string  `json:"source_epoch"`
		Mode              string  `json:"receive_mode"`
		Revision          int64   `json:"connection_revision"`
		TelegramAPIOrigin string  `json:"telegram_api_origin"`
		Origin            *string `json:"public_origin"`
		OriginStatus      string  `json:"origin_status"`
	}{PreflightReceiveModesPolicy, scope, epoch, mode, connectionRevision, apiOrigin, origin, originStatus})
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

// ValidatePreflightChecksForMode keeps old persisted results on the original
// webhook-only rules. NOT_APPLICABLE is allowed only at fixed polling positions.
func ValidatePreflightChecksForMode(policy, mode string, checks []PreflightCheck) (string, error) {
	if policy == PreflightWeComPolicy {
		return validateWeComPreflightChecks(mode, checks)
	}
	if policy == "" {
		if mode != "" {
			return "", ErrInvalidDocument
		}
		return ValidatePreflightChecks(checks)
	}
	if policy != PreflightReceiveModesPolicy || (mode != "webhook" && mode != "long_polling") {
		return "", ErrInvalidDocument
	}
	if mode == "webhook" {
		return ValidatePreflightChecks(checks)
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
	if (checks[0].Code == "BOT_TOKEN_MISSING") != (checks[1].Code == "NOT_EXECUTED") {
		return "", ErrInvalidDocument
	}
	if (checks[1].Code == "BOT_IDENTITY_MATCH") == (checks[3].Code == "NOT_EXECUTED") {
		return "", ErrInvalidDocument
	}
	read := checks[3].Code == "WEBHOOK_NONE" || checks[3].Code == "WEBHOOK_BLOCKS_LONG_POLLING"
	if read == (checks[4].Code == "NOT_EXECUTED") {
		return "", ErrInvalidDocument
	}
	outcome := "PASS"
	rank := map[string]int{"PASS": 0, "WARN": 1, "UNKNOWN": 2, "SKIPPED": 2, "FAIL": 3}
	for _, c := range checks[:6] {
		if c.Status == "NOT_APPLICABLE" {
			continue
		}
		if rank[c.Status] > rank[outcome] {
			outcome = c.Status
			if outcome == "SKIPPED" {
				outcome = "UNKNOWN"
			}
		}
	}
	return outcome, nil
}
