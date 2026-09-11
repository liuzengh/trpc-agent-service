// Package accountcatalog defines the non-secret Control account replica and its
// ordering rules. It owns no HTTP, SDK client, credential storage or lease SQL.
package accountcatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

const MaxRevision int64 = 9007199254740991
const MaxAccounts = 1000
const MaxSnapshotBytes = 2 << 20

var (
	ErrInvalid      = errors.New("account catalog: invalid protocol")
	ErrIntegrity    = errors.New("account catalog: integrity conflict")
	ErrUnavailable  = errors.New("account catalog: unavailable")
	ErrUnauthorized = errors.New("account catalog: unauthorized")
	ErrVersion      = errors.New("account catalog: configuration changed")
	ErrExpired      = errors.New("account catalog: request expired")
	id              = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	epoch           = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	digestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	decimal         = regexp.MustCompile(`^[1-9][0-9]*$`)
)

type Credential struct {
	Purpose    string `json:"purpose"`
	ID         string `json:"credential_id"`
	Version    int64  `json:"credential_version"`
	Configured bool   `json:"configured"`
}
type Config struct {
	ReceiveMode     string `json:"receive_mode,omitempty"`
	EndpointProfile string `json:"endpoint_profile,omitempty"`
	WebhookPath     string `json:"webhook_path,omitempty"`
	BotID           string `json:"bot_id,omitempty"`
}
type Account struct {
	TenantID           string       `json:"tenant_id"`
	ID                 string       `json:"account_id"`
	Provider           string       `json:"provider"`
	ProviderAccountID  string       `json:"provider_account_id"`
	Revision           int64        `json:"account_revision"`
	ConnectionRevision int64        `json:"connection_revision"`
	Enabled            bool         `json:"enabled"`
	MinRouteGeneration int64        `json:"min_route_generation"`
	Config             Config       `json:"config"`
	Credentials        []Credential `json:"credentials"`
}
type Snapshot struct {
	SchemaVersion int       `json:"schema_version"`
	ScopeID       string    `json:"scope_id"`
	SourceEpoch   string    `json:"source_epoch"`
	Revision      int64     `json:"snapshot_revision"`
	Digest        string    `json:"snapshot_digest"`
	Complete      bool      `json:"complete"`
	Accounts      []Account `json:"accounts"`
}

func ValidID(s string) bool      { return id.MatchString(s) }
func ValidEpoch(s string) bool   { return epoch.MatchString(s) }
func ValidRevision(n int64) bool { return n > 0 && n <= MaxRevision }
func ValidWebhookSecret(s string) bool {
	if len(s) < 1 || len(s) > 256 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func (a Account) Validate() error {
	if !ValidID(a.ID) || !ValidID(a.TenantID) || !ValidRevision(a.Revision) || !ValidRevision(a.ConnectionRevision) || a.ConnectionRevision > a.Revision || a.MinRouteGeneration < 0 || a.MinRouteGeneration > MaxRevision {
		return ErrInvalid
	}
	if len(a.ProviderAccountID) == 0 || len(a.ProviderAccountID) > 1024 || !utf8.ValidString(a.ProviderAccountID) || strings.IndexFunc(a.ProviderAccountID, func(c rune) bool { return unicode.IsSpace(c) || unicode.IsControl(c) }) >= 0 {
		return ErrInvalid
	}
	var purposes []string
	switch a.Provider {
	case "telegram":
		if a.Config.EndpointProfile != "" && a.Config.EndpointProfile != "test" {
			return ErrInvalid
		}
		if (a.Config.ReceiveMode != "" && a.Config.ReceiveMode != "webhook" && a.Config.ReceiveMode != "long_polling") || !decimal.MatchString(a.ProviderAccountID) || a.Config.BotID != "" || a.Config.WebhookPath != "/v1/telegram/"+a.ID {
			return ErrInvalid
		}
		purposes = []string{"telegram.bot_token", "telegram.webhook_secret"}
	case "wecom":
		if a.Config.EndpointProfile != "" || a.Config.ReceiveMode != "" || a.Config.WebhookPath != "" || a.Config.BotID != a.ProviderAccountID {
			return ErrInvalid
		}
		purposes = []string{"wecom.bot_secret"}
	default:
		return ErrInvalid
	}
	if len(a.Credentials) != len(purposes) {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for i, c := range a.Credentials {
		if c.Purpose != purposes[i] || !ValidID(c.ID) || seen[c.ID] || !ValidRevision(c.Version) || a.Enabled && !c.Configured && !(a.Provider == "telegram" && a.ReceiveMode() == "long_polling" && c.Purpose == "telegram.webhook_secret") {
			return ErrInvalid
		}
		seen[c.ID] = true
	}
	return nil
}

// ReceiveMode interprets legacy webhook-only snapshots without changing their
// serialized bytes or digest. New Control snapshots always set the mode.
func (a Account) ReceiveMode() string {
	if a.Provider != "telegram" {
		return ""
	}
	if a.Config.ReceiveMode == "" {
		return "webhook"
	}
	return a.Config.ReceiveMode
}
func (s Snapshot) ComputedDigest() (string, error) {
	body, err := json.Marshal(struct {
		ScopeID     string    `json:"scope_id"`
		SourceEpoch string    `json:"source_epoch"`
		Revision    int64     `json:"snapshot_revision"`
		Accounts    []Account `json:"accounts"`
	}{s.ScopeID, s.SourceEpoch, s.Revision, s.Accounts})
	if err != nil {
		return "", ErrInvalid
	}
	canonical, err := jcs.Transform(body)
	if err != nil {
		return "", ErrInvalid
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
func (s Snapshot) Validate() error {
	if s.SchemaVersion != 1 || !ValidID(s.ScopeID) || !ValidEpoch(s.SourceEpoch) || !ValidRevision(s.Revision) || !s.Complete || s.Accounts == nil || len(s.Accounts) > MaxAccounts || !digestPattern.MatchString(s.Digest) {
		return ErrInvalid
	}
	ids, physical, credentials := map[string]bool{}, map[string]bool{}, map[string]bool{}
	last := ""
	for _, a := range s.Accounts {
		if err := a.Validate(); err != nil {
			return err
		}
		key := a.Provider + "\x00" + a.ID
		pkey := a.Provider + "\x00" + a.ProviderAccountID
		if ids[a.ID] || physical[pkey] || (last != "" && key <= last) {
			return ErrInvalid
		}
		ids[a.ID] = true
		physical[pkey] = true
		last = key
		for _, c := range a.Credentials {
			if credentials[c.ID] {
				return ErrInvalid
			}
			credentials[c.ID] = true
		}
	}
	got, err := s.ComputedDigest()
	if err != nil {
		return err
	}
	if got != s.Digest {
		return ErrIntegrity
	}
	b, err := json.Marshal(s)
	if err != nil || len(b) > MaxSnapshotBytes {
		return ErrInvalid
	}
	return nil
}

// CheckSuccessor keeps an omitted account's identity and high-water marks.
// Reappearance requires a newer connection revision, never a new identity.
func CheckSuccessor(old, next Account, present bool) error {
	if (!present && next.ConnectionRevision <= old.ConnectionRevision) || old.ID != next.ID || old.TenantID != next.TenantID || old.Provider != next.Provider || old.ProviderAccountID != next.ProviderAccountID || next.Revision < old.Revision || next.ConnectionRevision < old.ConnectionRevision || next.MinRouteGeneration < old.MinRouteGeneration {
		return ErrIntegrity
	}
	if len(old.Credentials) != len(next.Credentials) {
		return ErrIntegrity
	}
	for i, c := range old.Credentials {
		n := next.Credentials[i]
		if c.ID != n.ID || c.Purpose != n.Purpose || n.Version < c.Version || (n.Version == c.Version && n.Configured != c.Configured) {
			return ErrIntegrity
		}
	}
	if next.ConnectionRevision == old.ConnectionRevision {
		x, y := old, next
		x.Revision = 0
		y.Revision = 0
		x.MinRouteGeneration = 0
		y.MinRouteGeneration = 0
		xb, _ := json.Marshal(x)
		yb, _ := json.Marshal(y)
		if string(xb) != string(yb) {
			return ErrIntegrity
		}
	}
	if next.Revision == old.Revision && next.ConnectionRevision != old.ConnectionRevision {
		return ErrIntegrity
	}
	return nil
}

type Classification string

const (
	Same       Classification = "SAME"
	Advance    Classification = "ADVANCE"
	Superseded Classification = "SUPERSEDED"
)

func Classify(start, current, received int64, knownDigest, receivedDigest string) (Classification, error) {
	if !ValidRevision(received) || start < 0 || current < start || current > MaxRevision {
		return "", ErrIntegrity
	}
	if knownDigest != "" && knownDigest != receivedDigest {
		return "", ErrIntegrity
	}
	if received < start {
		return "", ErrIntegrity
	}
	if received < current {
		return Superseded, nil
	}
	if received == current {
		return Same, nil
	}
	return Advance, nil
}
func SortAccounts(a []Account) {
	sort.Slice(a, func(i, j int) bool {
		if a[i].Provider == a[j].Provider {
			return a[i].ID < a[j].ID
		}
		return a[i].Provider < a[j].Provider
	})
}

type Use struct {
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
type ResolveRequest struct {
	SchemaVersion      int      `json:"schema_version"`
	ScopeID            string   `json:"scope_id"`
	SourceEpoch        string   `json:"source_epoch"`
	ConnectionRevision int64    `json:"connection_revision"`
	Uses               []Use    `json:"uses"`
	Consumer           Consumer `json:"consumer"`
}

func (r ResolveRequest) Validate(a Account) error {
	if a.Validate() != nil || !a.Enabled || r.SchemaVersion != 1 || !ValidID(r.ScopeID) || !ValidEpoch(r.SourceEpoch) || r.ConnectionRevision != a.ConnectionRevision || !ValidID(r.Consumer.InstanceID) {
		return ErrUnauthorized
	}
	var purposes []string
	switch r.Consumer.Kind {
	case "wecom_connection":
		if a.Provider != "wecom" || r.Consumer.OwnerEpoch == nil || *r.Consumer.OwnerEpoch < 1 || r.Consumer.RegistrationEpoch != nil {
			return ErrUnauthorized
		}
		purposes = []string{"wecom.bot_secret"}
	case "telegram_receiver":
		if a.Provider != "telegram" || r.Consumer.OwnerEpoch == nil || *r.Consumer.OwnerEpoch < 1 || *r.Consumer.OwnerEpoch > MaxRevision || r.Consumer.RegistrationEpoch != nil {
			return ErrUnauthorized
		}
		purposes = []string{"telegram.bot_token"}
	case "telegram_registration":
		if a.ReceiveMode() != "webhook" {
			return ErrUnauthorized
		}
		if a.Provider != "telegram" || r.Consumer.RegistrationEpoch == nil || *r.Consumer.RegistrationEpoch < 1 || r.Consumer.OwnerEpoch != nil {
			return ErrUnauthorized
		}
		purposes = []string{"telegram.bot_token", "telegram.webhook_secret"}
	case "telegram_webhook", "telegram_delivery":
		if a.Provider != "telegram" || r.Consumer.OwnerEpoch != nil || r.Consumer.RegistrationEpoch != nil {
			return ErrUnauthorized
		}
		if r.Consumer.Kind == "telegram_webhook" {
			if a.ReceiveMode() != "webhook" {
				return ErrUnauthorized
			}
			purposes = []string{"telegram.webhook_secret"}
		} else {
			purposes = []string{"telegram.bot_token"}
		}
	default:
		return ErrUnauthorized
	}
	if len(r.Uses) != len(purposes) {
		return ErrUnauthorized
	}
	for i, p := range purposes {
		u := r.Uses[i]
		found := false
		for _, c := range a.Credentials {
			if c.Purpose == p && c.Configured && u.Purpose == p && c.ID == u.ID && c.Version == u.Version {
				found = true
			}
		}
		if !found {
			return ErrUnauthorized
		}
	}
	return nil
}
