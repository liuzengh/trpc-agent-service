package datav1

import _ "embed"

// Schema documents the internal wire shape. Validate adds semantic target checks
// (host validity, reserved bucket aliases, exact endpoint form) beyond JSON Schema.
//
//go:embed backend-snapshot.schema.json
var Schema []byte
