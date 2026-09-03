// Package runtimecontext contains trusted routing data resolved by the
// platform before a message reaches the Agent runtime.
package runtimecontext

import (
	"errors"
	"fmt"
	"regexp"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Scope is the tenant/application/channel boundary for one Agent turn.
type Scope struct {
	TenantID         string
	AppID            string
	RevisionID       string
	ChannelType      string
	ChannelBindingID string
	StorageScope     string
}

// NewScope validates trusted control-plane identifiers and derives the
// tRPC-Agent-Go AppName used to isolate Session and Memory data.
func NewScope(
	tenantID string,
	appID string,
	revisionID string,
	channelType string,
	channelBindingID string,
) (Scope, error) {
	for name, value := range map[string]string{
		"tenant_id":          tenantID,
		"app_id":             appID,
		"revision_id":        revisionID,
		"channel_type":       channelType,
		"channel_binding_id": channelBindingID,
	} {
		if !identifierPattern.MatchString(value) {
			return Scope{}, fmt.Errorf("invalid runtime %s", name)
		}
	}
	return Scope{
		TenantID:         tenantID,
		AppID:            appID,
		RevisionID:       revisionID,
		ChannelType:      channelType,
		ChannelBindingID: channelBindingID,
		StorageScope:     "t/" + tenantID + "/a/" + appID,
	}, nil
}

// Validate detects forged or partially constructed runtime scopes.
func (s Scope) Validate() error {
	expected, err := NewScope(
		s.TenantID,
		s.AppID,
		s.RevisionID,
		s.ChannelType,
		s.ChannelBindingID,
	)
	if err != nil {
		return err
	}
	if s.StorageScope != expected.StorageScope {
		return errors.New("runtime storage scope does not match tenant and app")
	}
	return nil
}

// TutorialScope is the bootstrap route used by direct Runtime callers.
func TutorialScope() Scope {
	scope, err := NewScope(
		"tutorial-tenant",
		"tutorial-app",
		"tutorial-revision-1",
		"http",
		"tutorial-http-binding",
	)
	if err != nil {
		panic(err)
	}
	return scope
}
