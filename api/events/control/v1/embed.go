// Package controleventsv1 embeds the Control publication event schemas shared
// by the Control API, Outbox relay, and trusted runtime projection consumers.
package controleventsv1

import _ "embed"

// RuntimeManifestPublishedSchema is the closed RuntimeManifestPublished.v1
// event envelope schema.
//
//go:embed runtime-manifest-published.schema.json
var RuntimeManifestPublishedSchema []byte
