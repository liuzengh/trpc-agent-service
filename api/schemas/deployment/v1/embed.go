// Package deploymentv1 embeds the current Deployment Input, internal
// RuntimeManifest, and public redacted RuntimeManifest View schemas.
package deploymentv1

import _ "embed"

// InputSchema is the closed two-source Deployment Input V1 schema.
//
//go:embed deployment-input.schema.json
var InputSchema []byte

// ManifestSchema is the trusted internal RuntimeManifest V1 schema.
//
//go:embed runtime-manifest.schema.json
var ManifestSchema []byte

// ManifestViewSchema is the public credential-ID-free manifest projection.
//
//go:embed runtime-manifest-view.schema.json
var ManifestViewSchema []byte
