package application

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

const WorkloadAudience = "control-channel-v1"

// WorkloadPrincipal is supplied only by the mTLS authentication adapter from a
// configured URI SAN mapping. No request field can create or enlarge this identity.
type WorkloadPrincipal struct {
	PrincipalID, ScopeID, InstanceID, Audience string
	Consumers                                  []string
}
type ResolveRequest struct {
	SchemaVersion      int                    `json:"schema_version"`
	ScopeID            string                 `json:"scope_id"`
	SourceEpoch        string                 `json:"source_epoch"`
	ConnectionRevision int64                  `json:"connection_revision"`
	Uses               []domain.CredentialUse `json:"uses"`
	Consumer           domain.Consumer        `json:"consumer"`
}
type ResolvedValue struct {
	Purpose string `json:"purpose"`
	ID      string `json:"credential_id"`
	Version int64  `json:"credential_version"`
	Value   string `json:"value"`
}

func (ResolvedValue) String() string   { return "[resolved channel credential]" }
func (ResolvedValue) GoString() string { return "[resolved channel credential]" }

type ResolveResponse struct {
	ScopeID            string          `json:"scope_id"`
	SourceEpoch        string          `json:"source_epoch"`
	TenantID           string          `json:"tenant_id"`
	AccountID          string          `json:"account_id"`
	ConnectionRevision int64           `json:"connection_revision"`
	Values             []ResolvedValue `json:"values"`
}
type Observation struct {
	ReceiveMode        string          `json:"receive_mode,omitempty"`
	ScopeID            string          `json:"scope_id"`
	SourceEpoch        string          `json:"source_epoch"`
	TenantID           string          `json:"tenant_id"`
	AccountID          string          `json:"account_id"`
	Provider           domain.Provider `json:"provider"`
	ConnectionRevision int64           `json:"connection_revision"`
	InstanceID         string          `json:"instance_id"`
	InstanceEpoch      string          `json:"instance_epoch"`
	ReportSequence     int64           `json:"report_sequence"`
	State              string          `json:"state"`
	ReasonCode         string          `json:"reason_code"`
	ObservedAt         time.Time       `json:"observed_at"`
	OwnerEpoch         *int64          `json:"owner_epoch,omitempty"`
}
type ObservationsRequest struct {
	SchemaVersion int           `json:"schema_version"`
	Observations  []Observation `json:"observations"`
}
type ObservationView struct {
	ReceiveMode        string    `json:"receive_mode,omitempty"`
	ConnectionRevision int64     `json:"connection_revision"`
	InstanceID         string    `json:"instance_id"`
	InstanceEpoch      string    `json:"instance_epoch"`
	ReportSequence     int64     `json:"report_sequence"`
	State              string    `json:"state"`
	ReasonCode         string    `json:"reason_code"`
	ObservedAt         time.Time `json:"observed_at"`
	OwnerEpoch         *int64    `json:"owner_epoch,omitempty"`
	ReceivedAt         time.Time `json:"received_at"`
	EffectiveState     string    `json:"effective_state"`
}

type RuntimeStore interface {
	ReadSnapshot(context.Context, string) (domain.Snapshot, error)
	WithCredentials(context.Context, string, string, string, func(domain.Account, []domain.CredentialRecord, string) error) error
	SaveObservations(context.Context, string, []Observation) error
	PruneObservations(context.Context) error
}
type RuntimeService struct {
	store        RuntimeStore
	cipher       CredentialCipher
	scope, epoch string
}

func NewRuntimeService(store RuntimeStore, cipher CredentialCipher, scope, epoch string) (*RuntimeService, error) {
	if store == nil || cipher == nil || !domain.ValidID(scope) || !domain.ValidEpoch(epoch) {
		return nil, ErrDependencyUnavailable
	}
	return &RuntimeService{store, cipher, scope, epoch}, nil
}
func (s *RuntimeService) authorize(p WorkloadPrincipal) error {
	if p.PrincipalID == "" || p.Audience != WorkloadAudience || p.ScopeID != s.scope || !domain.ValidID(p.InstanceID) {
		return ErrWorkloadDenied
	}
	return nil
}
func (s *RuntimeService) ReadSnapshot(ctx context.Context, p WorkloadPrincipal) (domain.Snapshot, error) {
	if err := s.authorize(p); err != nil {
		return domain.Snapshot{}, err
	}
	value, err := s.store.ReadSnapshot(ctx, p.ScopeID)
	if err != nil {
		return value, err
	}
	if value.SourceEpoch != s.epoch || value.ScopeID != p.ScopeID {
		return domain.Snapshot{}, ErrEpochMismatch
	}
	return value, nil
}
func (s *RuntimeService) ResolveCredentials(ctx context.Context, p WorkloadPrincipal, tenant, account string, input ResolveRequest) (ResolveResponse, error) {
	if err := s.authorize(p); err != nil {
		return ResolveResponse{}, err
	}
	if input.ScopeID != p.ScopeID || input.Consumer.InstanceID != p.InstanceID || !slices.Contains(p.Consumers, input.Consumer.Kind) {
		return ResolveResponse{}, ErrWorkloadDenied
	}
	if input.SourceEpoch != s.epoch {
		return ResolveResponse{}, ErrEpochMismatch
	}
	if !domain.ValidID(tenant) || !domain.ValidID(account) {
		return ResolveResponse{}, invalid("")
	}
	raw, err := json.Marshal(input)
	if err != nil || channelv1.Validate("credentials-resolve-request.schema.json", raw) != nil {
		return ResolveResponse{}, invalid("")
	}
	var response ResolveResponse
	err = s.store.WithCredentials(ctx, p.ScopeID, tenant, account, func(a domain.Account, credentials []domain.CredentialRecord, epoch string) error {
		if epoch != input.SourceEpoch {
			return ErrEpochMismatch
		}
		if a.ConnectionRevision != input.ConnectionRevision {
			return &domain.Error{Code: domain.CredentialVersionConflict}
		}
		if err := domain.MatchCredentialUses(a, input.Consumer, input.Uses, credentials); err != nil {
			return err
		}
		values := make([]ResolvedValue, 0, len(input.Uses))
		for _, use := range input.Uses {
			for _, c := range credentials {
				if c.Meta.Purpose != use.Purpose {
					continue
				}
				aad, err := c.AAD()
				if err != nil {
					return err
				}
				plaintext, err := s.cipher.Decrypt(ctx, c.KeyID, aad, c.Ciphertext)
				if err != nil {
					return ErrDependencyUnavailable
				}
				value := string(plaintext)
				clear(plaintext)
				edit := domain.CredentialEdit{Action: "replace", Value: &value}
				if err = edit.Validate(a.Provider, c.Meta.Purpose); err != nil {
					return &domain.Error{Code: domain.SourceIntegrity}
				}
				values = append(values, ResolvedValue{Purpose: use.Purpose, ID: use.ID, Version: use.Version, Value: value})
			}
		}
		response = ResolveResponse{ScopeID: p.ScopeID, SourceEpoch: epoch, TenantID: tenant, AccountID: account, ConnectionRevision: a.ConnectionRevision, Values: values}
		return nil
	})
	if err != nil {
		return ResolveResponse{}, err
	}
	return response, nil
}
func (s *RuntimeService) ReportObservations(ctx context.Context, p WorkloadPrincipal, input ObservationsRequest) error {
	if err := s.authorize(p); err != nil {
		return err
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > 128*1024 || channelv1.Validate("observations.schema.json", raw) != nil {
		return invalid("/observations")
	}
	seen := map[string]bool{}
	for _, o := range input.Observations {
		if o.ScopeID != p.ScopeID || o.InstanceID != p.InstanceID {
			return ErrWorkloadDenied
		}
		if o.SourceEpoch != s.epoch {
			return ErrEpochMismatch
		}
		key := o.TenantID + "\x00" + o.AccountID + "\x00" + o.InstanceID + "\x00" + o.InstanceEpoch
		if seen[key] {
			return invalid("/observations")
		}
		seen[key] = true
	}
	return s.store.SaveObservations(ctx, p.ScopeID, input.Observations)
}
func (s *RuntimeService) PruneObservations(ctx context.Context) error {
	return s.store.PruneObservations(ctx)
}
