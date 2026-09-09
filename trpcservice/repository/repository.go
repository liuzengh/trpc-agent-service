// Package repository defines persistence contracts for the control plane.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrNotFound indicates that a tenant-scoped record does not exist.
	ErrNotFound = errors.New("repository: record not found")
	// ErrVersionConflict indicates that optimistic configuration publication lost a race.
	ErrVersionConflict = errors.New("repository: config version conflict")
)

// ConfigRecord is one immutable tenant configuration publication.
type ConfigRecord struct {
	TenantID       string
	TenantName     string
	TenantEnabled  bool
	Version        tenant.ConfigVersion
	Payload        []byte
	SHA256         string
	RolledBackFrom *tenant.ConfigVersion
	Apps           []tenant.AgentApp
	CreatedBy      string
	CreatedAt      time.Time
}

// Store persists immutable tenant configuration versions.
type Store interface {
	PublishConfig(ctx context.Context, record ConfigRecord, expected tenant.ConfigVersion) (ConfigRecord, error)
	ListConfigVersions(ctx context.Context, tenantID string) ([]ConfigRecord, error)
	GetConfigVersion(ctx context.Context, tenantID string, version tenant.ConfigVersion) (ConfigRecord, error)
	// GetCurrentConfig returns the tenant's published head version.
	GetCurrentConfig(ctx context.Context, tenantID string) (ConfigRecord, error)
}

func cloneRecord(record ConfigRecord) ConfigRecord {
	record.Payload = append([]byte(nil), record.Payload...)
	record.Apps = make([]tenant.AgentApp, len(record.Apps))
	for i := range record.Apps {
		record.Apps[i] = record.Apps[i].Clone()
	}
	if record.RolledBackFrom != nil {
		value := *record.RolledBackFrom
		record.RolledBackFrom = &value
	}
	return record
}
