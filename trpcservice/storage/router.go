// Package storage defines tenant-scoped backend routing contracts.
package storage

import "context"

type Domain string
type Capability string
type CapabilitySet map[Capability]bool

const (
	CapabilityAtomicTurnCommit     Capability = "atomic_turn_commit"
	CapabilityStrongReadAfterWrite Capability = "strong_ryw"
	CapabilitySummaryCAS           Capability = "summary_cas"
)

type BackendBinding struct {
	TenantID         string
	ConfigVersion    int64
	Domain           Domain
	BackendProfileID string
	BackendVersion   int64
	Required         CapabilitySet
}

type ScopedBackend interface{ TenantID() string }
type Router interface {
	Resolve(context.Context, Domain) (ScopedBackend, error)
}
