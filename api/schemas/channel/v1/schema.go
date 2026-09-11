// Package channelv1 owns the closed Control/Gateway channel HTTP wire contracts.
// Validation never returns rejected values or detailed credential-bearing errors.
package channelv1

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"sync"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed *.schema.json
var Files embed.FS

var ErrInvalidDocument = errors.New("invalid channel protocol document")
var ErrUnknownSchema = errors.New("unknown channel protocol schema")
var compiled struct {
	sync.Once
	schemas map[string]*jsonschema.Schema
	err     error
}

const location = "https://jfsas.dev/schemas/channel/v1/"

func compile() {
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	names, err := Files.ReadDir(".")
	if err != nil {
		compiled.err = err
		return
	}
	for _, name := range names {
		raw, err := Files.ReadFile(name.Name())
		if err != nil {
			compiled.err = err
			return
		}
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			compiled.err = err
			return
		}
		if err = c.AddResource(location+name.Name(), value); err != nil {
			compiled.err = err
			return
		}
	}
	compiled.schemas = make(map[string]*jsonschema.Schema)
	for _, name := range names {
		schema, err := c.Compile(location + name.Name())
		if err != nil {
			compiled.err = err
			return
		}
		compiled.schemas[name.Name()] = schema
	}
}

// Validate rejects duplicate keys, invalid UTF-8, unknown/case-varied fields,
// missing fields, nulls and values outside the schema. Business cross-field
// identities, sorting, digest and authorization are checked by the owner.
func Validate(name string, raw []byte) error {
	compiled.Do(compile)
	if compiled.err != nil {
		return ErrUnknownSchema
	}
	schema, ok := compiled.schemas[name]
	if !ok || name == "common.schema.json" {
		return ErrUnknownSchema
	}
	if !utf8.Valid(raw) {
		return ErrInvalidDocument
	}
	if _, err := jcs.Transform(raw); err != nil {
		return ErrInvalidDocument
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return ErrInvalidDocument
	}
	if err = schema.Validate(value); err != nil {
		return ErrInvalidDocument
	}
	return validatePreflightSemantics(name, raw)
}

// Decode uses the same closed wire validation before decoding an adapter DTO.
func Decode(name string, raw []byte, target any) error {
	if err := Validate(name, raw); err != nil {
		return err
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return ErrInvalidDocument
	}
	return nil
}
