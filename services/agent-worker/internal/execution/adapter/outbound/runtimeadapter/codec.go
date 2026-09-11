package runtimeadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

func decodeBatch(raw []byte, out *batchWire) error {
	if !utf8.Valid(raw) {
		return errors.New("invalid batch")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing batch data")
	}
	if err := exactFields(raw, "tenant_id", "profile_id", "profile_revision_number", "run_id", "attempt_id", "worker_id", "lease_epoch", "manifest_id", "manifest_digest", "credentials"); err != nil {
		return err
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return err
	}
	var credentials []json.RawMessage
	if err := json.Unmarshal(top["credentials"], &credentials); err != nil || credentials == nil {
		return errors.New("invalid credential entries")
	}
	for _, c := range credentials {
		if err := exactFields(c, "credential_id", "purpose", "audience_digest", "credential_revision", "value"); err != nil {
			return err
		}
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}
func exactFields(raw []byte, fields ...string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || len(object) != len(fields) {
		return errors.New("invalid object fields")
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return errors.New("invalid object fields")
		}
	}
	return nil
}
func uniqueValue(d *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("batch nesting exceeded")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			keyToken, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || seen[key] {
				return errors.New("duplicate batch key")
			}
			seen[key] = true
			if err = uniqueValue(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("invalid batch object")
		}
	case '[':
		for d.More() {
			if err = uniqueValue(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("invalid batch array")
		}
	default:
		return errors.New("invalid batch delimiter")
	}
	return nil
}
