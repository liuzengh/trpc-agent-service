// Package runtimeprofilev1 embeds the public RuntimeProfileSpec V1 JSON Schema
// so tests and consumers validate the exact versioned protocol checked in here.
package runtimeprofilev1

import _ "embed"

// Schema is the canonical RuntimeProfileSpec V1 JSON Schema.
//
//go:embed runtime-profile-spec.schema.json
var Schema []byte
