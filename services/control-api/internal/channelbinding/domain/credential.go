package domain

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"
)

type CredentialEdit struct {
	Action string  `json:"action"`
	Value  *string `json:"value,omitempty"`
}

// String and GoString protect accidental diagnostic formatting. Wire encoding is
// deliberately write-only and must never be used in receipts or ordinary reads.
func (CredentialEdit) String() string   { return "[credential edit]" }
func (CredentialEdit) GoString() string { return "[credential edit]" }
func (e CredentialEdit) Validate(provider Provider, purpose string) error {
	if !ValidPurpose(provider, purpose) {
		return failure(InputInvalid, "/purpose")
	}
	switch e.Action {
	case "replace":
		if e.Value == nil || len(*e.Value) == 0 || len(*e.Value) > MaxCredentialBytes || !utf8.ValidString(*e.Value) {
			return failure(InputInvalid, "/value")
		}
		if purpose == TelegramWebhookSecret && !webhookSecretPattern.MatchString(*e.Value) {
			return failure(InputInvalid, "/value")
		}
	case "keep", "clear":
		if e.Value != nil {
			return failure(InputInvalid, "/value")
		}
	default:
		return failure(InputInvalid, "/action")
	}
	return nil
}

type CredentialMeta struct {
	Purpose    string `json:"purpose"`
	ID         string `json:"credential_id"`
	Version    int64  `json:"credential_version"`
	Configured bool   `json:"configured"`
}
type CredentialStatus struct {
	Purpose    string `json:"purpose"`
	Version    int64  `json:"credential_version"`
	Configured bool   `json:"configured"`
}

func PublicCredentialStatus(values []CredentialMeta) []CredentialStatus {
	result := make([]CredentialStatus, 0, len(values))
	for _, v := range values {
		result = append(result, CredentialStatus{v.Purpose, v.Version, v.Configured})
	}
	slices.SortFunc(result, func(a, b CredentialStatus) int {
		if a.Purpose < b.Purpose {
			return -1
		}
		if a.Purpose > b.Purpose {
			return 1
		}
		return 0
	})
	return result
}

// CredentialRecord is module-private storage material, never a wire DTO.
type CredentialRecord struct {
	TenantID   string         `json:"-"`
	AccountID  string         `json:"-"`
	Provider   Provider       `json:"-"`
	Meta       CredentialMeta `json:"-"`
	KeyID      string         `json:"-"`
	Ciphertext []byte         `json:"-"`
}

func (CredentialRecord) String() string   { return "[encrypted channel credential]" }
func (CredentialRecord) GoString() string { return "[encrypted channel credential]" }
func (r CredentialRecord) AAD() ([]byte, error) {
	if !ValidID(r.TenantID) || !ValidID(r.AccountID) || !ValidID(r.Meta.ID) || !ValidVersion(r.Meta.Version) || !ValidPurpose(r.Provider, r.Meta.Purpose) {
		return nil, failure(SourceIntegrity, "/credentials")
	}
	return json.Marshal([]any{"channel-account-v1", r.TenantID, r.AccountID, r.Provider, r.Meta.Purpose, r.Meta.ID, r.Meta.Version})
}
func ValidateCredentialSet(provider Provider, values []CredentialMeta, configured bool, mode ...string) error {
	required := AllowedPurposes(provider)
	if len(required) == 0 || len(values) != len(required) {
		return failure(CredentialRequired, "/credentials")
	}
	seen := map[string]bool{}
	ids := map[string]bool{}
	for _, v := range values {
		if !ValidPurpose(provider, v.Purpose) || seen[v.Purpose] || !ValidID(v.ID) || ids[v.ID] || !ValidVersion(v.Version) {
			return failure(SourceIntegrity, "/credentials")
		}
		if configured && slices.Contains(RequiredPurposes(provider, mode...), v.Purpose) && !v.Configured {
			return failure(CredentialRequired, "/credentials")
		}
		seen[v.Purpose] = true
		ids[v.ID] = true
	}
	return nil
}

// PlanCredentialUpdate allocates metadata versions but never encrypts or mutates
// an existing ciphertext. The application encrypts using the returned new AAD.
func PlanCredentialUpdate(a Account, r CredentialRecord, expectedAccount, expectedCredential int64, edit CredentialEdit, now time.Time) (Account, CredentialRecord, bool, error) {
	if !ValidVersion(expectedAccount) || a.Revision != expectedAccount {
		return a, r, false, failure(RevisionConflict, "/expected_account_revision")
	}
	if !ValidVersion(expectedCredential) || r.Meta.Version != expectedCredential {
		return a, r, false, failure(CredentialVersionConflict, "/expected_credential_version")
	}
	if r.TenantID != a.TenantID || r.AccountID != a.ID || r.Provider != a.Provider {
		return a, r, false, failure(SourceIntegrity, "")
	}
	if err := edit.Validate(a.Provider, r.Meta.Purpose); err != nil {
		return a, r, false, err
	}
	if edit.Action == "keep" {
		return a, r, false, nil
	}
	if edit.Action == "clear" && a.Enabled {
		return a, r, false, failure(AccountMustBeDisabled, "/action")
	}
	// Explicit clear of an already-cleared value is a semantic NOOP.
	if edit.Action == "clear" && !r.Meta.Configured {
		return a, r, false, nil
	}
	version, err := NextVersion(r.Meta.Version)
	if err != nil {
		return a, r, false, err
	}
	next, err := a.AdvanceConnection(now)
	if err != nil {
		return a, r, false, err
	}
	r.Meta.Version = version
	r.Meta.Configured = edit.Action == "replace"
	r.Ciphertext = nil
	r.KeyID = ""
	return next, r, true, nil
}

type CredentialUse struct {
	Purpose string `json:"purpose"`
	ID      string `json:"credential_id"`
	Version int64  `json:"credential_version"`
}
type Consumer struct {
	Kind              string `json:"kind"`
	InstanceID        string `json:"instance_id"`
	OwnerEpoch        *int64 `json:"owner_epoch,omitempty"`
	RegistrationEpoch *int64 `json:"registration_epoch,omitempty"`
}

func ConsumerPurposes(provider Provider, c Consumer) ([]string, error) {
	if !ValidID(c.InstanceID) {
		return nil, failure(InputInvalid, "/consumer/instance_id")
	}
	switch c.Kind {
	case "wecom_connection":
		if provider != WeCom || c.OwnerEpoch == nil || !ValidVersion(*c.OwnerEpoch) || c.RegistrationEpoch != nil {
			break
		}
		return []string{WeComBotSecret}, nil
	case "telegram_webhook", "telegram_delivery":
		if provider != Telegram || c.OwnerEpoch != nil || c.RegistrationEpoch != nil {
			break
		}
		if c.Kind == "telegram_webhook" {
			return []string{TelegramWebhookSecret}, nil
		}
		return []string{TelegramBotToken}, nil
	case "telegram_receiver":
		if provider != Telegram || c.OwnerEpoch == nil || !ValidVersion(*c.OwnerEpoch) || c.RegistrationEpoch != nil {
			break
		}
		return []string{TelegramBotToken}, nil
	case "telegram_registration":
		if provider != Telegram || c.OwnerEpoch != nil || (c.RegistrationEpoch != nil && !ValidVersion(*c.RegistrationEpoch)) {
			break
		}
		return RequiredPurposes(Telegram), nil
	}
	return nil, failure(InputInvalid, "/consumer")
}
func ValidateUses(provider Provider, c Consumer, uses []CredentialUse) error {
	purposes, err := ConsumerPurposes(provider, c)
	if err != nil {
		return err
	}
	if len(uses) != len(purposes) {
		return failure(InputInvalid, "/uses")
	}
	seen := map[string]bool{}
	for _, u := range uses {
		if !slices.Contains(purposes, u.Purpose) || seen[u.Purpose] || !ValidID(u.ID) || !ValidVersion(u.Version) {
			return failure(InputInvalid, "/uses")
		}
		seen[u.Purpose] = true
	}
	return nil
}

// MatchCredentialUses validates the entire precise set before any decryption.
func MatchCredentialUses(a Account, c Consumer, uses []CredentialUse, records []CredentialRecord) error {
	if !a.Enabled {
		return failure(AccountDisabled, "")
	}
	if (c.Kind == "telegram_registration" || c.Kind == "telegram_webhook") && a.Config.ReceiveMode != Webhook {
		return failure(InputInvalid, "/consumer")
	}
	if err := ValidateUses(a.Provider, c, uses); err != nil {
		return err
	}
	for _, u := range uses {
		matches := 0
		for _, r := range records {
			if r.Meta.Purpose != u.Purpose {
				continue
			}
			matches++
			if r.TenantID != a.TenantID || r.AccountID != a.ID || r.Provider != a.Provider {
				return failure(SourceIntegrity, "/credentials")
			}
			if r.Meta.ID != u.ID || r.Meta.Version != u.Version || !r.Meta.Configured {
				return failure(CredentialVersionConflict, "/uses")
			}
		}
		if matches != 1 {
			return failure(CredentialVersionConflict, "/uses")
		}
	}
	return nil
}

var _ fmt.Stringer = CredentialRecord{}
