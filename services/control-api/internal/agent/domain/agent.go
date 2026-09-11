// Package domain contains Agent authoring and immutable versioning rules. It
// has no HTTP, persistence, message-bus, or runtime-framework dependencies.
package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrInvalidAgent = errors.New("invalid agent")

// Agent is the stable Tenant-scoped identity for an authored Agent.
type Agent struct {
	ID                  string
	TenantID            string
	Name                string
	Description         string
	LatestVersionNumber *int64
	CreatedBy           string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func NewAgent(id, tenantID, name, description, createdBy string, now time.Time) (Agent, error) {
	agent := Agent{
		ID: id, TenantID: tenantID, Name: strings.TrimSpace(name),
		Description: strings.TrimSpace(description), CreatedBy: createdBy,
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
	if err := agent.Validate(); err != nil {
		return Agent{}, err
	}
	return agent, nil
}

func (a *Agent) UpdateMetadata(name, description string, now time.Time) error {
	if a == nil {
		return ErrInvalidAgent
	}
	a.Name = strings.TrimSpace(name)
	a.Description = strings.TrimSpace(description)
	a.UpdatedAt = now.UTC()
	return a.Validate()
}

func (a Agent) Validate() error {
	if a.ID == "" || a.TenantID == "" || a.CreatedBy == "" || a.Name == "" ||
		utf8.RuneCountInString(a.Name) > 128 || utf8.RuneCountInString(a.Description) > 4096 {
		return ErrInvalidAgent
	}
	return nil
}
