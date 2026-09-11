package controleventsv1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const ManifestSubject = "control.runtime-manifest.published.v1"
const ManifestStream = "RUNTIME_MANIFESTS_V1"
const MaxManifestEventBytes = 1024 * 1024

var ErrInvalidManifestEvent = errors.New("invalid runtime manifest publication event")

type RuntimeManifestPublishedEvent struct {
	SchemaVersion        string                       `json:"schema_version"`
	EventType            string                       `json:"event_type"`
	EventID              string                       `json:"event_id"`
	TenantID             string                       `json:"tenant_id"`
	DeploymentID         string                       `json:"deployment_id"`
	DeploymentRevisionID string                       `json:"deployment_revision_id"`
	RevisionNumber       int64                        `json:"revision_number"`
	OccurredAt           time.Time                    `json:"occurred_at"`
	Manifest             deploymentv1.RuntimeManifest `json:"manifest"`
	TraceContext         *ManifestTraceContext        `json:"trace_context,omitempty"`
}
type ManifestTraceContext struct {
	Traceparent string `json:"traceparent"`
	Tracestate  string `json:"tracestate,omitempty"`
}

var compileManifestEvent = sync.OnceValues(func() (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	for location, raw := range map[string][]byte{
		"https://jfsas.dev/schemas/deployment/v1/runtime-manifest.schema.json":       deploymentv1.ManifestSchema,
		"https://jfsas.dev/events/control/v1/runtime-manifest-published.schema.json": RuntimeManifestPublishedSchema,
	} {
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, err
		}
		if err := c.AddResource(location, doc); err != nil {
			return nil, err
		}
	}
	return c.Compile("https://jfsas.dev/events/control/v1/runtime-manifest-published.schema.json")
})

func DecodeRuntimeManifestPublishedEvent(raw []byte) (RuntimeManifestPublishedEvent, error) {
	var e RuntimeManifestPublishedEvent
	if len(raw) == 0 || len(raw) > MaxManifestEventBytes || !utf8.Valid(raw) {
		return e, ErrInvalidManifestEvent
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return e, ErrInvalidManifestEvent
	}
	validator, err := compileManifestEvent()
	if err != nil {
		return e, fmt.Errorf("compile manifest publication schema: %w", err)
	}
	if err = validator.Validate(doc); err != nil {
		return e, ErrInvalidManifestEvent
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return e, ErrInvalidManifestEvent
	}
	dec := json.NewDecoder(bytes.NewReader(canonical))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&e); err != nil {
		return RuntimeManifestPublishedEvent{}, ErrInvalidManifestEvent
	}
	m := e.Manifest
	if e.TenantID != m.TenantID || e.DeploymentID != m.DeploymentID || e.DeploymentRevisionID != m.DeploymentRevisionID || e.RevisionNumber != m.RevisionNumber || !e.OccurredAt.Equal(m.PublishedAt) {
		return RuntimeManifestPublishedEvent{}, ErrInvalidManifestEvent
	}
	if _, err = deploymentv1.VerifyRuntimeManifest(m); err != nil {
		return RuntimeManifestPublishedEvent{}, errors.Join(ErrInvalidManifestEvent, err)
	}
	return e, nil
}

// ManifestEventDigest is the canonical stable event digest. Unlike the route
// codec this accepts the independently versioned 1 MiB publication envelope.
func ManifestEventDigest(raw []byte) (string, error) {
	if _, err := DecodeRuntimeManifestPublishedEvent(raw); err != nil {
		return "", err
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return "", ErrInvalidManifestEvent
	}
	h := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}
