package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"github.com/gowebpki/jcs"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func ValidDigest(value string) bool { return digestPattern.MatchString(value) }
func CanonicalJSON(value any) (json.RawMessage, string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, "", failure(SourceIntegrity, "")
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, "", failure(SourceIntegrity, "")
	}
	sum := sha256.Sum256(canonical)
	return canonical, "sha256:" + hex.EncodeToString(sum[:]), nil
}

type SnapshotAccount struct {
	TenantID           string           `json:"tenant_id"`
	AccountID          string           `json:"account_id"`
	Provider           Provider         `json:"provider"`
	ProviderAccountID  string           `json:"provider_account_id"`
	AccountRevision    int64            `json:"account_revision"`
	ConnectionRevision int64            `json:"connection_revision"`
	Enabled            bool             `json:"enabled"`
	MinRouteGeneration int64            `json:"min_route_generation"`
	Config             ConnectionConfig `json:"config"`
	Credentials        []CredentialMeta `json:"credentials"`
}

func ProjectSnapshotAccount(a Account, credentials []CredentialMeta) (SnapshotAccount, error) {
	if err := a.Validate(); err != nil {
		return SnapshotAccount{}, err
	}
	if err := ValidateCredentialSet(a.Provider, credentials, a.Enabled, a.Config.ReceiveMode); err != nil {
		return SnapshotAccount{}, err
	}
	return SnapshotAccount{a.TenantID, a.ID, a.Provider, a.ProviderAccountID, a.Revision, a.ConnectionRevision, a.Enabled, a.MinRouteGeneration, a.Config, slices.Clone(credentials)}, nil
}

type Snapshot struct {
	SchemaVersion int               `json:"schema_version"`
	ScopeID       string            `json:"scope_id"`
	SourceEpoch   string            `json:"source_epoch"`
	Revision      int64             `json:"snapshot_revision"`
	Digest        string            `json:"snapshot_digest"`
	Complete      bool              `json:"complete"`
	Accounts      []SnapshotAccount `json:"accounts"`
}

func snapshotDigest(s Snapshot) (string, error) {
	_, digest, err := CanonicalJSON(struct {
		ScopeID     string            `json:"scope_id"`
		SourceEpoch string            `json:"source_epoch"`
		Revision    int64             `json:"snapshot_revision"`
		Accounts    []SnapshotAccount `json:"accounts"`
	}{s.ScopeID, s.SourceEpoch, s.Revision, s.Accounts})
	return digest, err
}
func accountCompare(a, b SnapshotAccount) int {
	if d := strings.Compare(string(a.Provider), string(b.Provider)); d != 0 {
		return d
	}
	return strings.Compare(a.AccountID, b.AccountID)
}
func credentialCompare(a, b CredentialMeta) int { return strings.Compare(a.Purpose, b.Purpose) }
func NewSnapshot(scope, epoch string, revision int64, accounts []SnapshotAccount) (Snapshot, error) {
	s := Snapshot{SchemaVersion: 1, ScopeID: scope, SourceEpoch: epoch, Revision: revision, Complete: true, Accounts: append([]SnapshotAccount{}, accounts...)}
	for i := range s.Accounts {
		s.Accounts[i].Credentials = slices.Clone(s.Accounts[i].Credentials)
		slices.SortFunc(s.Accounts[i].Credentials, credentialCompare)
	}
	slices.SortFunc(s.Accounts, accountCompare)
	if err := s.validateContent(); err != nil {
		return Snapshot{}, err
	}
	digest, err := snapshotDigest(s)
	if err != nil {
		return Snapshot{}, err
	}
	s.Digest = digest
	raw, err := json.Marshal(s)
	if err != nil || len(raw) > MaxSnapshotBytes {
		return Snapshot{}, failure("CHANNEL_LIMIT_EXCEEDED", "")
	}
	if err := channelv1.Validate("account-snapshot.schema.json", raw); err != nil {
		return Snapshot{}, failure(SourceIntegrity, "")
	}
	return s, nil
}
func (s Snapshot) validateContent() error {
	if s.SchemaVersion != 1 || !s.Complete || !ValidID(s.ScopeID) || !ValidEpoch(s.SourceEpoch) || !ValidVersion(s.Revision) || s.Accounts == nil || len(s.Accounts) > MaxAccounts {
		return failure(SourceIntegrity, "")
	}
	ids, physical, credentialIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i, a := range s.Accounts {
		if i > 0 && accountCompare(s.Accounts[i-1], a) >= 0 {
			return failure(SourceIntegrity, "/accounts")
		}
		if !ValidID(a.TenantID) || !ValidID(a.AccountID) || ids[a.AccountID] || !ValidVersion(a.AccountRevision) || !ValidVersion(a.ConnectionRevision) || a.ConnectionRevision > a.AccountRevision || a.MinRouteGeneration < 0 || a.MinRouteGeneration > MaxVersion {
			return failure(SourceIntegrity, "/accounts")
		}
		ids[a.AccountID] = true
		value, err := NormalizeProviderAccountID(a.Provider, a.ProviderAccountID)
		if err != nil || value != a.ProviderAccountID {
			return failure(SourceIntegrity, "/accounts")
		}
		key := string(a.Provider) + "\x00" + value
		if physical[key] {
			return failure(SourceIntegrity, "/accounts")
		}
		physical[key] = true
		if a.Provider == Telegram {
			if (a.Config.EndpointProfile != "" && a.Config.EndpointProfile != "test") || a.Config.WebhookPath != "/v1/telegram/"+a.AccountID || a.Config.BotID != "" || (a.Config.ReceiveMode != "" && !ValidReceiveMode(a.Config.ReceiveMode)) {
				return failure(SourceIntegrity, "/accounts/config")
			}
		} else if a.Config.EndpointProfile != "" || a.Config.BotID != value || a.Config.WebhookPath != "" || a.Config.ReceiveMode != "" {
			return failure(SourceIntegrity, "/accounts/config")
		}
		if err := ValidateCredentialSet(a.Provider, a.Credentials, a.Enabled, a.Config.ReceiveMode); err != nil {
			return failure(SourceIntegrity, "/accounts/credentials")
		}
		for j, c := range a.Credentials {
			if j > 0 && credentialCompare(a.Credentials[j-1], c) >= 0 || credentialIDs[c.ID] || c.Version > a.ConnectionRevision {
				return failure(SourceIntegrity, "/accounts/credentials")
			}
			credentialIDs[c.ID] = true
		}
	}
	return nil
}
func DecodeSnapshot(raw []byte) (Snapshot, error) {
	if len(raw) > MaxSnapshotBytes {
		return Snapshot{}, failure("CHANNEL_LIMIT_EXCEEDED", "")
	}
	var s Snapshot
	if err := channelv1.Decode("account-snapshot.schema.json", raw, &s); err != nil {
		return s, failure(SourceIntegrity, "")
	}
	if err := s.validateContent(); err != nil {
		return Snapshot{}, err
	}
	digest, err := snapshotDigest(s)
	if err != nil || digest != s.Digest {
		return Snapshot{}, failure(SourceIntegrity, "/snapshot_digest")
	}
	return s, nil
}
