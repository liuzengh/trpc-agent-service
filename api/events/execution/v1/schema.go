// Package executionv1 owns the versioned execution wire contract and codec.
// Generated DTOs are transport values; they are not service domain entities.
package executionv1

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:generate go run ./generate

const (
	RunRequestedSubject  = "execution.run-requested.v1"
	MaxRunRequestedBytes = 1 << 20
	MaxInputTextBytes    = 65536
)

// RunRequestedSchema is the exact versioned JSON Schema used by the codec.
//
//go:embed run-requested.schema.json
var RunRequestedSchema []byte

var ErrInvalidRunRequested = errors.New("invalid RunRequested v1 event")

var compiledSchema = sync.OnceValues(func() (*jsonschema.Schema, error) { return compile("") })
var compiledReplyContextSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	return compile("#/$defs/ReplyContext")
})

func compile(fragment string) (*jsonschema.Schema, error) {
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(RunRequestedSchema))
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	const location = "https://trpc-agent-service.local/events/execution/v1/run-requested.schema.json"
	if err = compiler.AddResource(location, value); err != nil {
		return nil, err
	}
	return compiler.Compile(location + fragment)
}

// EncodeRunRequested validates the complete immutable event before serialization.
// Optional empty fields are omitted by the generated DTO. Required empty values,
// arbitrary provider objects and inconsistent route/input pairs are rejected.
// The result is deterministic RFC 8785 JSON, suitable for the durable Outbox.
func EncodeRunRequested(event dto.RunRequested) ([]byte, error) {
	// encoding/json replaces invalid UTF-8 in Go strings. Reject it before marshal.
	if !validStrings(reflect.ValueOf(event)) {
		return nil, invalid("invalid Unicode")
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return nil, invalid("encoding")
	}
	canonical, _, err := decode(raw)
	return canonical, err
}

// DecodeRunRequested rejects unknown fields, duplicates, nulls, malformed UTF-8,
// unpaired UTF-16 escapes, trailing JSON and out-of-range numbers. Schema validation
// uses json.Number before normalization, so generation is never silently rounded.
func DecodeRunRequested(raw []byte) (dto.RunRequested, error) {
	_, event, err := decode(raw)
	return event, err
}

func decode(raw []byte) ([]byte, dto.RunRequested, error) {
	var zero dto.RunRequested
	if len(raw) == 0 || len(raw) > MaxRunRequestedBytes || !validJSONUnicode(raw) {
		return nil, zero, invalid("size or Unicode")
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, zero, invalid("JSON syntax")
	}
	schema, err := compiledSchema()
	if err != nil {
		return nil, zero, errors.New("execution schema unavailable")
	}
	if err = schema.Validate(value); err != nil {
		return nil, zero, invalid("schema")
	}
	// All numeric fields are bounded by the schema to the exact IEEE-754 integer
	// range; all external identifiers stay strings. JCS also rejects duplicate keys.
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, zero, invalid("noncanonical JSON")
	}
	var event dto.RunRequested
	if err = json.Unmarshal(canonical, &event); err != nil {
		return nil, zero, invalid("typed decoding")
	}
	if err = validateRelations(event); err != nil {
		return nil, zero, err
	}
	return canonical, event, nil
}

func validateRelations(event dto.RunRequested) error {
	in := event.Input
	if event.Route.Provider != in.Key.Provider || event.Route.AccountID != in.Key.AccountID {
		return invalid("route/input identity mismatch")
	}
	if len(in.Text) > MaxInputTextBytes || strings.TrimSpace(in.Text) == "" {
		return invalid("text boundary")
	}
	received, err := time.Parse(time.RFC3339Nano, in.ReceivedAt)
	if err != nil || received.IsZero() {
		return invalid("received_at")
	}
	if in.ReplyContext == nil {
		return invalid("missing reply context")
	}
	reply := in.ReplyContext
	switch in.Key.Provider {
	case "telegram":
		if reply.ChatID != in.ConversationID || reply.MessageThreadID != in.ThreadID {
			return invalid("reply address mismatch")
		}
		if !decimalID(in.Key.EventID, true, false) || !decimalID(in.SenderID, false, false) || !decimalID(in.ConversationID, false, true) {
			return invalid("Telegram identity")
		}
		if in.ThreadID != "" && !decimalID(in.ThreadID, false, false) {
			return invalid("Telegram thread identity")
		}
		if reply.SourceMessageID != "" && !decimalID(reply.SourceMessageID, false, false) {
			return invalid("Telegram message identity")
		}
	case "wecom":
		if reply.ChatIDOrUserID != in.ConversationID {
			return invalid("reply address mismatch")
		}
		contextTime, err := time.Parse(time.RFC3339Nano, reply.ReceivedAt)
		if err != nil || !contextTime.Equal(received) {
			return invalid("reply context time mismatch")
		}
	}
	return nil
}

func decimalID(value string, zero, negative bool) bool {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != value {
		return false
	}
	return (zero && n == 0) || n > 0 || (negative && n < 0)
}

// DecodeReplyContext is the narrow adapter mapping seam for a stored normalized
// address. It rejects arbitrary provider payloads rather than dropping fields.
// The complete event encoder subsequently checks address/conversation consistency.
func DecodeReplyContext(provider string, raw []byte) (*dto.ReplyContext, error) {
	if len(raw) == 0 || len(raw) > 65536 || !validJSONUnicode(raw) {
		return nil, invalid("reply context")
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, invalid("reply context JSON")
	}
	validator, err := compiledReplyContextSchema()
	if err != nil {
		return nil, errors.New("execution reply schema unavailable")
	}
	if err = validator.Validate(value); err != nil {
		return nil, invalid("reply context schema")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, invalid("reply context object")
	}
	allowed := map[string]bool{}
	required := []string{}
	switch provider {
	case "telegram":
		allowed = map[string]bool{"chat_id": true, "message_thread_id": true, "source_message_id": true}
		required = []string{"chat_id"}
	case "wecom":
		allowed = map[string]bool{"chat_type": true, "chatid_or_userid": true, "callback_req_id": true, "received_at": true}
		required = []string{"chat_type", "chatid_or_userid", "received_at"}
	default:
		return nil, invalid("reply context provider")
	}
	for key, val := range object {
		s, ok := val.(string)
		if !allowed[key] || !ok || s == "" {
			return nil, invalid("reply context fields")
		}
	}
	for _, key := range required {
		if _, ok := object[key]; !ok {
			return nil, invalid("reply context missing field")
		}
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, invalid("reply context duplicate field")
	}
	var reply dto.ReplyContext
	if err = json.Unmarshal(canonical, &reply); err != nil {
		return nil, invalid("reply context decoding")
	}
	if provider == "telegram" {
		if !decimalID(reply.ChatID, false, true) || (reply.MessageThreadID != "" && !decimalID(reply.MessageThreadID, false, false)) || (reply.SourceMessageID != "" && !decimalID(reply.SourceMessageID, false, false)) {
			return nil, invalid("reply context Telegram identity")
		}
	} else {
		if reply.ChatType != "single" && reply.ChatType != "group" {
			return nil, invalid("reply context chat type")
		}
		at, err := time.Parse(time.RFC3339Nano, reply.ReceivedAt)
		if err != nil || at.IsZero() {
			return nil, invalid("reply context received_at")
		}
	}
	return &reply, nil
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidRunRequested, reason) }

func validStrings(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return utf8.ValidString(v.String())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !validStrings(v.Field(i)) {
				return false
			}
		}
	case reflect.Pointer:
		if !v.IsNil() {
			return validStrings(v.Elem())
		}
	}
	return true
}

// JSON permits Unicode escapes but Go's decoder replaces malformed surrogate
// pairs. Check the raw escape sequence before any decoder can make that repair.
func validJSONUnicode(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	inString := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n < 0xd800 || n > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
