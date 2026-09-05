package vector

import "context"

// DocumentIdentity is the strictly controlled scalar projection identity read
// from a derived index for maintenance observation. It deliberately carries no
// vector and no source content.
type DocumentIdentity struct {
	TenantID        string
	DocumentID      string
	SourceType      string
	SourceID        string
	ProjectionScope string
	SourceVersion   int64
	SourceSequence  int64
	ContentHash     string
	Operation       VectorOperation
	Model           string
	ModelVersion    string
	Dimension       int
	SchemaVersion   string
}

// Ref rebuilds the server-owned document reference from the identity. It
// fails closed unless every field is present and the server-derived document
// ID matches, so identities can never be turned into cross-tenant tasks.
func (i DocumentIdentity) Ref() (VectorDocumentRef, error) {
	ref := VectorDocumentRef{
		TenantID: i.TenantID, SourceType: i.SourceType, SourceID: i.SourceID,
		ProjectionScope: i.ProjectionScope, SourceVersion: i.SourceVersion,
		SourceSequence: i.SourceSequence, ContentHash: i.ContentHash,
		Operation: i.Operation, Deleted: i.Operation == OperationDelete,
		DocumentID: i.DocumentID, Model: i.Model, ModelVersion: i.ModelVersion,
		Dimension: i.Dimension, SchemaVersion: i.SchemaVersion,
	}
	if err := ref.Validate(); err != nil {
		return VectorDocumentRef{}, err
	}
	return ref, nil
}

// IdentityReader is the internal maintenance boundary over a derived index.
// It reads only bounded, strictly controlled scalar identity fields. It is
// additive to the P1-06A public VectorStore contract, which stays unchanged.
// Implementations must derive the tenant predicate from the trusted context.
type IdentityReader interface {
	// InspectIdentities reads the identities of the given server-owned
	// document IDs. Absent documents are simply not present in the result.
	InspectIdentities(ctx context.Context, documentIDs []string) (map[string]DocumentIdentity, error)
	// ListIdentities returns up to limit identities of the trusted tenant in
	// stable document-ID order, for bounded orphan observation.
	ListIdentities(ctx context.Context, limit int) ([]DocumentIdentity, error)
}
