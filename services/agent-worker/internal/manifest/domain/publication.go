package domain

import "errors"

var (
	ErrMissing  = errors.New("manifest projection missing")
	ErrConflict = errors.New("manifest projection identity conflict")
	ErrCapacity = errors.New("manifest projection capacity reached")
)

// Publication is validated at the inbound protocol adapter. The stored complete
// envelope remains immutable; it is not a public redacted Manifest View.
type Publication struct {
	EventID, EventDigest                                                      string
	TenantID, ManifestID, DeploymentRevisionID, ContentDigest, EnvelopeDigest string
	Envelope                                                                  []byte
}
