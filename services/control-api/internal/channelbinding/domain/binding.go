package domain

import (
	"slices"
	"time"
)

type TargetSelector struct {
	DeploymentID   string `json:"deployment_id"`
	RevisionNumber int64  `json:"revision_number"`
}
type PublishedTarget struct {
	TenantID             string `json:"tenant_id"`
	DeploymentID         string `json:"deployment_id"`
	RevisionNumber       int64  `json:"revision_number"`
	DeploymentRevisionID string `json:"deployment_revision_id"`
	ManifestID           string `json:"manifest_ref"`
	ManifestDigest       string `json:"manifest_digest"`
}

// MaxCanarySubjects keeps the complete route event below MaxRouteEventBytes even
// when every identifier uses its maximum encoded length.
const MaxCanarySubjects = 100

// TrafficRollout adds one explicitly published canary target to the Binding's
// stable target. PercentageBasisPoints is evaluated only after an explicit
// subject match, using ID as the stable assignment salt.
type TrafficRollout struct {
	ID                    string          `json:"rollout_id"`
	Target                PublishedTarget `json:"target"`
	PercentageBasisPoints int64           `json:"percentage_basis_points"`
	CanarySubjects        []string        `json:"canary_subjects"`
}

func (r TrafficRollout) Validate(tenant string, stable PublishedTarget) error {
	if !ValidID(r.ID) || r.Target.Validate(tenant) != nil || sameRuntimeTarget(r.Target, stable) || r.PercentageBasisPoints < 0 || r.PercentageBasisPoints > 10000 || r.CanarySubjects == nil || len(r.CanarySubjects) > MaxCanarySubjects {
		return failure(InputInvalid, "/traffic")
	}
	for i, subject := range r.CanarySubjects {
		if !ValidID(subject) || i > 0 && r.CanarySubjects[i-1] >= subject {
			return failure(InputInvalid, "/traffic/canary_subjects")
		}
	}
	return nil
}

func sameRuntimeTarget(a, b PublishedTarget) bool {
	return a.DeploymentRevisionID == b.DeploymentRevisionID && a.ManifestID == b.ManifestID && a.ManifestDigest == b.ManifestDigest
}

func (t PublishedTarget) Validate(tenant string) error {
	if t.TenantID != tenant || !ValidID(t.TenantID) || !ValidID(t.DeploymentID) || !ValidVersion(t.RevisionNumber) || !ValidID(t.DeploymentRevisionID) || !ValidID(t.ManifestID) || !ValidDigest(t.ManifestDigest) {
		return failure(TargetIntegrity, "/target")
	}
	return nil
}
func (t PublishedTarget) Selector() TargetSelector {
	return TargetSelector{t.DeploymentID, t.RevisionNumber}
}

type Binding struct {
	TenantID  string          `json:"tenant_id"`
	ID        string          `json:"binding_id"`
	AccountID string          `json:"account_id"`
	Revision  int64           `json:"binding_revision"`
	Enabled   bool            `json:"enabled"`
	Target    PublishedTarget `json:"target"`
	Traffic   *TrafficRollout `json:"traffic,omitempty"`
	CreatedBy string          `json:"created_by"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func NewBinding(id, actor string, a Account, target PublishedTarget, now time.Time) (Binding, error) {
	if !ValidID(id) || !ValidID(actor) {
		return Binding{}, failure(InputInvalid, "")
	}
	if err := target.Validate(a.TenantID); err != nil {
		return Binding{}, err
	}
	return Binding{TenantID: a.TenantID, ID: id, AccountID: a.ID, Revision: 1, Target: target, CreatedBy: actor, CreatedAt: now, UpdatedAt: now}, nil
}
func (b Binding) SetTarget(expected int64, target PublishedTarget, now time.Time) (Binding, bool, error) {
	if !ValidVersion(expected) || expected != b.Revision {
		return b, false, failure(BindingRevisionConflict, "/expected_binding_revision")
	}
	if err := target.Validate(b.TenantID); err != nil {
		return b, false, err
	}
	if target == b.Target {
		return b, false, nil
	}
	revision, err := NextVersion(b.Revision)
	if err != nil {
		return b, false, err
	}
	b.Revision = revision
	b.Target = target
	b.Traffic = nil
	b.UpdatedAt = now
	return b, true, nil
}

func (b Binding) SetTraffic(expected int64, rolloutID string, target PublishedTarget, percentage int64, subjects []string, now time.Time) (Binding, bool, error) {
	if !ValidVersion(expected) || expected != b.Revision {
		return b, false, failure(BindingRevisionConflict, "/expected_binding_revision")
	}
	if b.Traffic != nil && b.Traffic.Target == target {
		rolloutID = b.Traffic.ID
	}
	copySubjects := slices.Clone(subjects)
	slices.Sort(copySubjects)
	rollout := TrafficRollout{ID: rolloutID, Target: target, PercentageBasisPoints: percentage, CanarySubjects: copySubjects}
	if err := rollout.Validate(b.TenantID, b.Target); err != nil {
		return b, false, err
	}
	if b.Traffic != nil && b.Traffic.ID == rollout.ID && b.Traffic.Target == rollout.Target && b.Traffic.PercentageBasisPoints == rollout.PercentageBasisPoints && slices.Equal(b.Traffic.CanarySubjects, rollout.CanarySubjects) {
		return b, false, nil
	}
	revision, err := NextVersion(b.Revision)
	if err != nil {
		return b, false, err
	}
	b.Revision = revision
	b.Traffic = &rollout
	b.UpdatedAt = now
	return b, true, nil
}
func (b Binding) SetEnabled(expected int64, enabled bool, a Account, credentials []CredentialMeta, now time.Time) (Binding, bool, error) {
	if !ValidVersion(expected) || b.Revision != expected {
		return b, false, failure(BindingRevisionConflict, "/expected_binding_revision")
	}
	if a.TenantID != b.TenantID || a.ID != b.AccountID {
		return b, false, failure(SourceIntegrity, "")
	}
	if enabled {
		if !a.Enabled {
			return b, false, failure(AccountDisabled, "")
		}
		if err := ValidateCredentialSet(a.Provider, credentials, true, a.Config.ReceiveMode); err != nil {
			return b, false, err
		}
		if err := b.Target.Validate(a.TenantID); err != nil {
			return b, false, err
		}
		if b.Traffic != nil {
			if err := b.Traffic.Validate(a.TenantID, b.Target); err != nil {
				return b, false, err
			}
		}
	}
	if enabled == b.Enabled {
		return b, false, nil
	}
	revision, err := NextVersion(b.Revision)
	if err != nil {
		return b, false, err
	}
	b.Revision = revision
	b.Enabled = enabled
	b.UpdatedAt = now
	return b, true, nil
}
