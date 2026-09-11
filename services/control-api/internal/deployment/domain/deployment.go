package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrInvalidDeployment = errors.New("invalid deployment")

// Deployment is the stable tenant-scoped identity. Published revisions are
// immutable records; LatestRevisionNumber is publication progress, not a
// traffic switch.
type Deployment struct {
	ID                   string    `json:"id"`
	TenantID             string    `json:"tenant_id"`
	Name                 string    `json:"name"`
	Description          string    `json:"description"`
	MetadataRevision     int64     `json:"metadata_revision"`
	LatestRevisionNumber *int64    `json:"latest_revision_number"`
	CreatedBy            string    `json:"created_by"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func NewDeployment(id, tenantID, name, description, createdBy string, now time.Time) (Deployment, error) {
	deployment := Deployment{
		ID: id, TenantID: tenantID, Name: strings.TrimSpace(name),
		Description: strings.TrimSpace(description), MetadataRevision: 1,
		CreatedBy: createdBy, CreatedAt: now.UTC().Truncate(time.Microsecond),
		UpdatedAt: now.UTC().Truncate(time.Microsecond),
	}
	if err := deployment.Validate(); err != nil {
		return Deployment{}, err
	}
	return deployment, nil
}

// UpdateMetadata applies a partial update. It reports whether the canonical
// metadata changed; callers must not advance the CAS for a semantic no-op.
func (d *Deployment) UpdateMetadata(name, description *string, now time.Time) (bool, error) {
	if d == nil || (name == nil && description == nil) {
		return false, ErrInvalidDeployment
	}
	nextName, nextDescription := d.Name, d.Description
	if name != nil {
		nextName = strings.TrimSpace(*name)
	}
	if description != nil {
		nextDescription = strings.TrimSpace(*description)
	}
	if nextName == d.Name && nextDescription == d.Description {
		return false, d.Validate()
	}
	next := *d
	next.Name, next.Description = nextName, nextDescription
	next.MetadataRevision++
	next.UpdatedAt = now.UTC().Truncate(time.Microsecond)
	if err := next.Validate(); err != nil {
		return false, err
	}
	*d = next
	return true, nil
}

func (d Deployment) Validate() error {
	if d.ID == "" || d.TenantID == "" || d.CreatedBy == "" || d.Name == "" ||
		d.MetadataRevision <= 0 || utf8.RuneCountInString(d.Name) > 128 ||
		utf8.RuneCountInString(d.Description) > 4096 || d.CreatedAt.IsZero() || d.UpdatedAt.IsZero() {
		return ErrInvalidDeployment
	}
	if d.LatestRevisionNumber != nil && *d.LatestRevisionNumber <= 0 {
		return ErrInvalidDeployment
	}
	return nil
}
