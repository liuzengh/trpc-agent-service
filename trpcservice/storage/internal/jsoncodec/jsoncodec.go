// Package jsoncodec owns the JSON format shared by SQL storage adapters.
package jsoncodec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Encode serializes a control-plane value and preserves encoding errors.
func Encode(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode control-plane value: %w", err)
	}
	return encoded, nil
}

// Decode accepts exactly one JSON value with no unknown fields. Empty storage
// payloads retain the legacy empty-object default. Adapters redact errors.
func Decode(data []byte, destination any) error {
	if len(data) == 0 {
		data = []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON data")
	}
	return nil
}
