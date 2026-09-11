package domain

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
)

// RuntimeTarget is private platform configuration, not a tenant-managed object.
// One definition can serve every authorized tenant sharing the same backend.
// It excludes credentials. Snapshot binds Tenant only after directory resolution.
type RuntimeTarget struct {
	BackendID       string                 `json:"backend_id"`
	BackendRevision uint64                 `json:"backend_revision"`
	Kind            datav1.Kind            `json:"kind"`
	Adapter         string                 `json:"adapter"`
	Isolation       string                 `json:"isolation"`
	Limits          datav1.Limits          `json:"limits"`
	PostgreSQL      *datav1.PostgresTarget `json:"postgresql,omitempty"`
	Redis           *datav1.RedisTarget    `json:"redis,omitempty"`
	Qdrant          *datav1.QdrantTarget   `json:"qdrant,omitempty"`
	S3              *datav1.S3Target       `json:"s3,omitempty"`
}

func (t RuntimeTarget) snapshot(tenant string) datav1.Snapshot {
	return datav1.Snapshot{SchemaVersion: datav1.Version, TenantID: tenant, BackendID: t.BackendID, BackendRevision: t.BackendRevision, Kind: t.Kind, Adapter: t.Adapter, Isolation: t.Isolation, Limits: t.Limits, PostgreSQL: t.PostgreSQL, Redis: t.Redis, Qdrant: t.Qdrant, S3: t.S3}.Clone()
}

// RuntimeCatalog pairs immutable metadata and private targets. It never falls
// back to another revision and does not expose private targets through List.
type RuntimeCatalog struct {
	directory *Catalog
	targets   map[string]RuntimeTarget
}

func NewRuntimeCatalog(directory *Catalog, targets []RuntimeTarget) (*RuntimeCatalog, error) {
	if directory == nil {
		return nil, ErrInvalid
	}
	c := &RuntimeCatalog{directory: directory, targets: make(map[string]RuntimeTarget, len(targets))}
	for _, t := range targets {
		if _, ok := c.targets[t.BackendID]; ok {
			return nil, ErrInvalid
		}
		e, ok := directory.entries[t.BackendID]
		if !ok || e.Revision != t.BackendRevision || string(e.Kind) != string(t.Kind) {
			return nil, ErrInvalid
		}
		// Each actual configured tenant is checked; no placeholder tenant is accepted
		// as proof of the deployed target contract.
		for _, tenant := range e.TenantIDs {
			if t.snapshot(tenant).Validate() != nil {
				return nil, ErrInvalid
			}
			for _, role := range e.Roles {
				if _, err := t.snapshot(tenant).ForRole(string(role)); err != nil {
					return nil, ErrInvalid
				}
			}
		}
		s := t.snapshot(e.TenantIDs[0])
		t.PostgreSQL = s.PostgreSQL
		t.Redis = s.Redis
		t.Qdrant = s.Qdrant
		t.S3 = s.S3
		c.targets[t.BackendID] = t
	}
	for id, e := range directory.entries {
		if e.Enabled {
			if _, ok := c.targets[id]; !ok {
				return nil, ErrInvalid
			}
		}
	}
	return c, nil
}
func (c *RuntimeCatalog) ResolveSnapshot(tenant string, selection Selection) (datav1.Snapshot, error) {
	if c == nil {
		return datav1.Snapshot{}, ErrNotAvailable
	}
	if _, err := c.directory.Resolve(tenant, selection); err != nil {
		return datav1.Snapshot{}, err
	}
	t, ok := c.targets[selection.BackendID]
	if !ok {
		return datav1.Snapshot{}, ErrUnavailable
	}
	s, err := t.snapshot(tenant).ForRole(string(selection.Role))
	if err != nil {
		return datav1.Snapshot{}, ErrInvalid
	}
	return s, nil
}
