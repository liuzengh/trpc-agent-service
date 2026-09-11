// Package domain contains Runtime Profile authoring and immutable revision rules.
// It has no HTTP, persistence, message-bus, or runtime-framework dependencies.
package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrInvalidRuntimeProfile = errors.New("invalid runtime profile")

// RuntimeProfile is the stable Tenant-scoped identity for reusable runtime
// configuration. Published revisions are stored separately and are immutable.
type RuntimeProfile struct {
	ID                   string
	TenantID             string
	Name                 string
	Description          string
	LatestRevisionNumber *int64
	CreatedBy            string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

func NewRuntimeProfile(
	id, tenantID, name, description, createdBy string,
	now time.Time,
) (RuntimeProfile, error) {
	profile := RuntimeProfile{
		ID: id, TenantID: tenantID, Name: strings.TrimSpace(name),
		Description: strings.TrimSpace(description), CreatedBy: createdBy,
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
	if err := profile.Validate(); err != nil {
		return RuntimeProfile{}, err
	}
	return profile, nil
}

func (p *RuntimeProfile) UpdateMetadata(name, description string, now time.Time) error {
	if p == nil {
		return ErrInvalidRuntimeProfile
	}
	p.Name = strings.TrimSpace(name)
	p.Description = strings.TrimSpace(description)
	p.UpdatedAt = now.UTC()
	return p.Validate()
}

func (p RuntimeProfile) Validate() error {
	if p.ID == "" || p.TenantID == "" || p.CreatedBy == "" || p.Name == "" ||
		utf8.RuneCountInString(p.Name) > 128 ||
		utf8.RuneCountInString(p.Description) > 4096 {
		return ErrInvalidRuntimeProfile
	}
	if p.LatestRevisionNumber != nil && *p.LatestRevisionNumber <= 0 {
		return ErrInvalidRuntimeProfile
	}
	return nil
}
