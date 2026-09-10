// Package execution owns the opaque work item passed from the durable queue to
// a worker.
package execution

import (
	"errors"
	"slices"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Job is a validated tenant-scoped work item. It can only be constructed by
// packages inside this module, after a durable queue record has been claimed.
type Job struct {
	requestID    string
	tenantSource gateway.TenantSource
	tenant       tenant.RuntimeContext
	message      gateway.Message
}

// NewJob constructs an opaque worker job from a durable queue record.
// It is intentionally in an internal package so external callers cannot
// construct work that bypasses Admission and the authoritative queue.
func NewJob(
	requestID string,
	tenantSource gateway.TenantSource,
	tenantContext tenant.RuntimeContext,
	message gateway.Message,
) (Job, error) {
	job := Job{
		requestID:    requestID,
		tenantSource: tenantSource,
		tenant:       tenantContext,
		message: gateway.Message{
			Text:         message.Text,
			ArtifactRefs: slices.Clone(message.ArtifactRefs),
		},
	}
	if err := job.Validate(); err != nil {
		return Job{}, err
	}
	return job, nil
}

// RequestID returns the durable request identifier.
func (j Job) RequestID() string {
	return j.requestID
}

// TenantSource returns the trusted boundary that supplied tenant routing.
func (j Job) TenantSource() gateway.TenantSource {
	return j.tenantSource
}

// Tenant returns the trusted tenant runtime context.
func (j Job) Tenant() tenant.RuntimeContext {
	return j.tenant
}

// Message returns a copy of the normalized user message.
func (j Job) Message() gateway.Message {
	return gateway.Message{
		Text:         j.message.Text,
		ArtifactRefs: slices.Clone(j.message.ArtifactRefs),
	}
}

// Validate checks the trusted routing fields required by workers.
func (j Job) Validate() error {
	if j.requestID == "" {
		return errors.New("request_id is required")
	}
	if j.tenantSource != gateway.TenantSourceAuthenticatedClaims &&
		j.tenantSource != gateway.TenantSourceVerifiedChannelBinding {
		return errors.New("tenant source is invalid")
	}
	if j.tenantSource == gateway.TenantSourceVerifiedChannelBinding {
		if j.tenant.Channel == "" {
			return errors.New("channel is required for verified channel binding")
		}
		if j.tenant.BindingID == "" {
			return errors.New("binding_id is required for verified channel binding")
		}
	}
	if err := j.tenant.Validate(); err != nil {
		return err
	}
	return j.message.Validate()
}

// PartitionKey returns the key used to serialize work for one session.
func (j Job) PartitionKey() (string, error) {
	return j.tenant.Scope().Key(
		"session",
		j.tenant.SessionPrincipalID,
		j.tenant.SessionID,
	)
}
