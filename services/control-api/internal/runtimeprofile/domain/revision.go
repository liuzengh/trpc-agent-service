package domain

import (
	"encoding/json"
	"time"
)

// ProfileRevision is an immutable publication containing one canonical
// RuntimeProfileSpec.
type ProfileRevision struct {
	ID                  string
	TenantID            string
	ProfileID           string
	RevisionNumber      int64
	SourceDraftRevision int64
	SchemaVersion       string
	Spec                json.RawMessage
	SpecDigest          string
	PublishedBy         string
	PublishedAt         time.Time
}

func (r ProfileRevision) Clone() ProfileRevision {
	r.Spec = append(json.RawMessage(nil), r.Spec...)
	return r
}
