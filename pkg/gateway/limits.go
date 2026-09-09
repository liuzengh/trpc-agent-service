//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"
)

const (
	// MaxWebhookBodyBytes bounds the raw webhook body an IM platform may post.
	MaxWebhookBodyBytes int64 = 1 << 20 // 1 MiB

	// MaxMessageContentBytes bounds the extracted message text accepted by the
	// Gateway. Content above this limit is rejected before it enters Inbox; it
	// must never be silently changed before a Worker sees it.
	MaxMessageContentBytes = 32 * 1024 // 32 KiB

	// MaxJSONDepth bounds nesting of a JSON webhook payload to stop stack
	// exhaustion from deeply recursive structures.
	MaxJSONDepth = 32
)

// ErrBodyTooLarge is returned when a webhook body exceeds MaxWebhookBodyBytes.
var ErrBodyTooLarge = errors.New("request body too large")

// ErrEmptyBody is returned when a webhook body carries no bytes.
var ErrEmptyBody = errors.New("empty request body")

// ErrJSONTooDeep is returned when a payload nests beyond MaxJSONDepth.
var ErrJSONTooDeep = errors.New("json nesting too deep")

// ErrInvalidUTF8 is returned when a payload is not valid UTF-8.
var ErrInvalidUTF8 = errors.New("payload is not valid utf-8")

// readLimitedBody consumes the request body under a hard size cap and rewinds it
// so downstream adapters can read the same bytes.
//
// It distinguishes three failure modes so the caller can map them to distinct
// HTTP status codes: oversize (413), empty (400) and transport failure (400).
func readLimitedBody(r *http.Request, limit int64) ([]byte, error) {
	if r.Body == nil {
		return nil, ErrEmptyBody
	}
	original := r.Body
	defer original.Close()

	// Reject on the declared length first: this avoids reading a large body at
	// all when the client is honest about its size.
	if r.ContentLength > limit {
		return nil, fmt.Errorf("%w: content-length %d exceeds %d", ErrBodyTooLarge, r.ContentLength, limit)
	}

	// MaxBytesReader guards the case of a missing or lying Content-Length. The
	// extra byte lets us tell "exactly at the limit" from "over the limit".
	limited := http.MaxBytesReader(nil, r.Body, limit+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) || int64(len(body)) > limit {
			return nil, fmt.Errorf("%w: exceeds %d bytes", ErrBodyTooLarge, limit)
		}
		return nil, fmt.Errorf("failed to read body: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%w: exceeds %d bytes", ErrBodyTooLarge, limit)
	}
	if len(body) == 0 {
		return nil, ErrEmptyBody
	}

	restoreBody(r, body)
	return body, nil
}

// restoreBody rewinds a consumed body so later readers observe the same bytes.
func restoreBody(r *http.Request, body []byte) {
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
}

// validateJSONDepth walks a JSON document with a streaming decoder and fails if
// nesting exceeds maxDepth. It does not allocate the decoded value, so a hostile
// payload cannot force a large heap allocation, and it never recurses, so it
// cannot exhaust the goroutine stack.
//
// Payloads that are not JSON at all are accepted here: XML channels such as
// WeCom carry non-JSON bodies and are bounded by readLimitedBody instead.
func validateJSONDepth(payload []byte, maxDepth int) error {
	lead := firstNonSpace(payload)
	if lead != '{' && lead != '[' && !isJSONScalarLead(lead) {
		return nil
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	if isJSONScalarLead(lead) {
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return fmt.Errorf("malformed json: %w", err)
		}
		if err := ensureJSONEOF(dec); err != nil {
			return err
		}
		return nil
	}
	depth := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			// A truncated document ends mid-structure: Token reports a plain EOF
			// rather than a syntax error, so unbalanced depth is the signal.
			if depth != 0 {
				return fmt.Errorf("malformed json: unexpected end of input at depth %d", depth)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("malformed json: %w", err)
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			depth++
			if depth > maxDepth {
				return fmt.Errorf("%w: exceeds %d levels", ErrJSONTooDeep, maxDepth)
			}
		case json.Delim('}'), json.Delim(']'):
			depth--
			if depth == 0 {
				// Token skips insignificant whitespace. Once the first object or
				// array is complete, any further token is a second top-level JSON
				// value and must be rejected before a custom adapter can decode only
				// the first value and enqueue a malformed request.
				return ensureJSONEOF(dec)
			}
		}
	}
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra json.RawMessage
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("malformed json: %w", err)
	}
	return fmt.Errorf("malformed json: multiple top-level values")
}

func isJSONScalarLead(lead byte) bool {
	return lead == '"' || lead == '-' || (lead >= '0' && lead <= '9') ||
		lead == 't' || lead == 'f' || lead == 'n'
}

// validatePayload applies the size-independent content checks shared by all
// channels: valid UTF-8 and bounded JSON nesting.
func validatePayload(payload []byte, maxDepth int) error {
	if !utf8.Valid(payload) {
		return ErrInvalidUTF8
	}
	return validateJSONDepth(payload, maxDepth)
}

// firstNonSpace returns the first non-whitespace byte, or 0 if there is none.
func firstNonSpace(b []byte) byte {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return c
		}
	}
	return 0
}
