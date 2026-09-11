// Package controleventsv1 owns the versioned Control route projection contract.
package controleventsv1

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/gowebpki/jcs"
	controlv1 "github.com/liuzengh/trpc-agent-service/gen/events/control/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:generate go run ./generate
//go:embed route-projected.schema.json
var RouteProjectionSchema []byte

// MaxEventBytes bounds the complete decoded event before schema validation.
const MaxEventBytes = 16384

var ErrInvalidEvent = errors.New("invalid control route projection event")
var compileSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	var document any
	if err := json.Unmarshal(RouteProjectionSchema, &document); err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	const location = "https://trpc-agent-service.local/events/control/v1/route-projected.schema.json"
	if err := c.AddResource(location, document); err != nil {
		return nil, err
	}
	return c.Compile(location)
})

// DecodeRouteProjectionEvent rejects unknown fields, invalid schema versions,
// missing enabled flags, fractional generations, and credential-bearing extras.
// The generated DTO is transport-only; adapters map it into owned domain values.
func DecodeRouteProjectionEvent(data []byte) (controlv1.RouteProjectionEvent, error) {
	var event controlv1.RouteProjectionEvent
	if len(data) == 0 || len(data) > MaxEventBytes {
		return event, ErrInvalidEvent
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return event, fmt.Errorf("%w: malformed JSON", ErrInvalidEvent)
	}
	validator, err := compileSchema()
	if err != nil {
		return event, fmt.Errorf("compile control route schema: %w", err)
	}
	if err = validator.Validate(document); err != nil {
		return event, fmt.Errorf("%w: schema mismatch", ErrInvalidEvent)
	}
	// Schema validation precedes canonicalization so out-of-range numbers are
	// rejected before JCS's IEEE-754 normalization. Valid integral encodings such
	// as 1.0 and 1e0 then decode consistently into generated integer fields. JCS
	// also rejects duplicate object keys rather than accepting last-key-wins.
	canonical, err := jcs.Transform(data)
	if err != nil {
		return event, fmt.Errorf("%w: ambiguous JSON", ErrInvalidEvent)
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&event); err != nil {
		return controlv1.RouteProjectionEvent{}, fmt.Errorf("%w: decode", ErrInvalidEvent)
	}
	if err = decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return controlv1.RouteProjectionEvent{}, fmt.Errorf("%w: trailing JSON", ErrInvalidEvent)
	}
	return event, nil
}
