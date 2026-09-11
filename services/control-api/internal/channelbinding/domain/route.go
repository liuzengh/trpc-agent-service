package domain

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
)

const RouteSubject = "control.channel-route.v1"
const RouteStream = "CHANNEL_ROUTES_V1"
const RouteEventType = "ChannelRouteProjected.v1"
const MaxRouteEventBytes = 16384

type Route struct {
	Provider             Provider        `json:"provider"`
	AccountID            string          `json:"account_id"`
	Generation           int64           `json:"generation"`
	TenantID             string          `json:"tenant_id,omitempty"`
	BindingID            string          `json:"binding_id,omitempty"`
	DeploymentRevisionID string          `json:"deployment_revision_id,omitempty"`
	ManifestRef          string          `json:"manifest_ref,omitempty"`
	ManifestDigest       string          `json:"manifest_digest,omitempty"`
	Traffic              *TrafficRollout `json:"traffic,omitempty"`
}
type RouteProjection struct {
	EventID       string `json:"event_id"`
	SchemaVersion int    `json:"schema_version"`
	Enabled       bool   `json:"enabled"`
	Route         Route  `json:"route"`
}
type RouteState struct {
	TenantID   string
	AccountID  string
	Generation int64
	Projection *RouteProjection
}

// AdvanceRoute compares the effective body excluding generation/event identity.
// The first Binding always emits generation 1, even when initially disabled.
func AdvanceRoute(previous RouteState, a Account, b *Binding, eventID string) (RouteState, bool, error) {
	if previous.TenantID != a.TenantID || previous.AccountID != a.ID || previous.Generation < 0 || previous.Generation > MaxVersion || a.MinRouteGeneration != previous.Generation {
		return previous, false, failure(SourceIntegrity, "")
	}
	if previous.Generation == 0 && previous.Projection != nil || previous.Generation > 0 && previous.Projection == nil {
		return previous, false, failure(SourceIntegrity, "")
	}
	if previous.Projection != nil {
		if err := previous.Projection.Validate(); err != nil {
			return previous, false, err
		}
		if previous.Projection.Route.AccountID != a.ID || previous.Projection.Route.Provider != a.Provider || previous.Projection.Route.Generation != previous.Generation || (previous.Projection.Enabled && previous.Projection.Route.TenantID != a.TenantID) {
			return previous, false, failure(SourceIntegrity, "")
		}
	}
	if b == nil {
		if previous.Generation != 0 {
			return previous, false, failure(SourceIntegrity, "")
		}
		return previous, false, nil
	}
	if b.TenantID != a.TenantID || b.AccountID != a.ID || !ValidID(b.ID) || !ValidVersion(b.Revision) {
		return previous, false, failure(SourceIntegrity, "")
	}
	enabled := a.Enabled && b.Enabled
	r := Route{Provider: a.Provider, AccountID: a.ID}
	if enabled {
		if err := b.Target.Validate(a.TenantID); err != nil {
			return previous, false, err
		}
		r.TenantID = a.TenantID
		r.BindingID = b.ID
		r.DeploymentRevisionID = b.Target.DeploymentRevisionID
		r.ManifestRef = b.Target.ManifestID
		r.ManifestDigest = b.Target.ManifestDigest
		if b.Traffic != nil {
			traffic := *b.Traffic
			traffic.CanarySubjects = slices.Clone(traffic.CanarySubjects)
			r.Traffic = &traffic
		}
	}
	if previous.Projection != nil {
		old := previous.Projection.Route
		old.Generation = 0
		if reflect.DeepEqual(old, r) && previous.Projection.Enabled == enabled {
			return previous, false, nil
		}
	}
	if previous.Generation == MaxVersion {
		return previous, false, failure(RouteGenerationExhausted, "")
	}
	r.Generation = previous.Generation + 1
	p := RouteProjection{EventID: eventID, SchemaVersion: 1, Enabled: enabled, Route: r}
	if err := p.Validate(); err != nil {
		return previous, false, err
	}
	return RouteState{TenantID: a.TenantID, AccountID: a.ID, Generation: r.Generation, Projection: &p}, true, nil
}
func (p RouteProjection) Validate() error {
	r := p.Route
	if p.SchemaVersion != 1 || !ValidID(p.EventID) || !ValidID(r.AccountID) || !ValidVersion(r.Generation) || (r.Provider != Telegram && r.Provider != WeCom) {
		return failure(SourceIntegrity, "/route")
	}
	if p.Enabled {
		if !ValidID(r.TenantID) || !ValidID(r.BindingID) || !ValidID(r.DeploymentRevisionID) || !ValidID(r.ManifestRef) || !ValidDigest(r.ManifestDigest) {
			return failure(SourceIntegrity, "/route")
		}
		stable := PublishedTarget{TenantID: r.TenantID, DeploymentRevisionID: r.DeploymentRevisionID, ManifestID: r.ManifestRef, ManifestDigest: r.ManifestDigest}
		if r.Traffic != nil {
			if err := r.Traffic.Validate(r.TenantID, stable); err != nil {
				return failure(SourceIntegrity, "/route/traffic")
			}
		}
	} else if r.TenantID != "" || r.BindingID != "" || r.DeploymentRevisionID != "" || r.ManifestRef != "" || r.ManifestDigest != "" || r.Traffic != nil {
		return failure(SourceIntegrity, "/route")
	}
	return nil
}
func (p RouteProjection) Encode() (json.RawMessage, string, error) {
	if err := p.Validate(); err != nil {
		return nil, "", err
	}
	raw, digest, err := CanonicalJSON(p)
	if err != nil {
		return nil, "", err
	}
	if len(raw) > MaxRouteEventBytes {
		return nil, "", failure("CHANNEL_LIMIT_EXCEEDED", "")
	}
	return raw, digest, nil
}

// ValidateStoredRoute accepts JSONB whitespace/key reordering, never altered data.
func ValidateStoredRoute(raw []byte, digest string) (RouteProjection, error) {
	if len(raw) > MaxRouteEventBytes {
		return RouteProjection{}, failure(SourceIntegrity, "")
	}
	var p RouteProjection
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return p, failure(SourceIntegrity, "")
	}
	encoded, calculated, err := p.Encode()
	if err != nil || calculated != digest {
		return RouteProjection{}, failure(SourceIntegrity, "")
	}
	// Canonicalization rejects duplicate keys/trailing bytes; equality also rejects
	// explicit null/empty optional target fields in a disabled tombstone.
	canonical, err := canonicalRaw(raw)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return RouteProjection{}, failure(SourceIntegrity, "")
	}
	return p, nil
}
