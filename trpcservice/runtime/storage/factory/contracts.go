// Package factory materializes tenant-scoped runtime storage capabilities from
// backend execution contracts. The backend package owns profile state; this
// package owns provider lookup, secret use, capability construction, and
// capability lifecycle.
package factory

import backendprofile "github.com/XnLemon/trpc-agent-service/trpcservice/backend"

// These aliases keep the materializer on the provider-neutral backend
// contracts without moving persisted profile state into runtime.
type (
	// Capability identifies one runtime storage capability.
	Capability = backendprofile.Capability
	// CapabilityBinding selects one provider for one capability.
	CapabilityBinding = backendprofile.CapabilityBinding
	// StorageFactoryInput is the secret-free storage construction input.
	StorageFactoryInput = backendprofile.StorageFactoryInput
)

// Capability constants identify the supported runtime storage capabilities.
const (
	CapabilitySession   = backendprofile.CapabilitySession
	CapabilityMemory    = backendprofile.CapabilityMemory
	CapabilitySummary   = backendprofile.CapabilitySummary
	CapabilityKnowledge = backendprofile.CapabilityKnowledge
	CapabilityArtifact  = backendprofile.CapabilityArtifact
	CapabilityAudit     = backendprofile.CapabilityAudit
)

// ErrInvalid is the backend-domain validation sentinel.
var ErrInvalid = backendprofile.ErrInvalid

func validCapability(capability Capability) bool {
	switch capability {
	case backendprofile.CapabilitySession, backendprofile.CapabilityMemory,
		backendprofile.CapabilitySummary, backendprofile.CapabilityKnowledge,
		backendprofile.CapabilityArtifact, backendprofile.CapabilityAudit:
		return true
	default:
		return false
	}
}
