package domain

import "time"

// ProfileRevisionSummary is the immutable revision metadata returned by list
// queries. It intentionally cannot carry the potentially large canonical Spec;
// callers must request one ProfileRevision to read and verify that document.
type ProfileRevisionSummary struct {
	ID                  string
	TenantID            string
	ProfileID           string
	RevisionNumber      int64
	SourceDraftRevision int64
	SchemaVersion       string
	SpecDigest          string
	PublishedBy         string
	PublishedAt         time.Time
}

// Summary projects a complete immutable revision onto its list-safe metadata.
func (r ProfileRevision) Summary() ProfileRevisionSummary {
	return ProfileRevisionSummary{
		ID: r.ID, TenantID: r.TenantID, ProfileID: r.ProfileID,
		RevisionNumber: r.RevisionNumber, SourceDraftRevision: r.SourceDraftRevision,
		SchemaVersion: r.SchemaVersion, SpecDigest: r.SpecDigest,
		PublishedBy: r.PublishedBy, PublishedAt: r.PublishedAt,
	}
}
