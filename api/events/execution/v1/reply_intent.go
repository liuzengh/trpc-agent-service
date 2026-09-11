package executionv1

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gowebpki/jcs"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:generate go run ./generate -schema reply-intent.schema.json -out ../../../../gen/events/execution/v1/reply_intent.go

const (
	ReplyIntentSubject  = "execution.reply-intent.v1"
	MaxReplyIntentBytes = 1 << 20
	MaxFinalTextBytes   = 65536
)

// ReplyIntentSchema defines the strict immutable text Final v1 business event.
// Transport trace headers and local Provider capabilities are not payload fields.
//
//go:embed reply-intent.schema.json
var ReplyIntentSchema []byte

var ErrInvalidReplyIntent = errors.New("invalid ReplyIntent v1 event")
var compiledReplyIntentSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(ReplyIntentSchema))
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	const location = "https://trpc-agent-service.local/events/execution/v1/reply-intent.schema.json"
	if err = compiler.AddResource(location, value); err != nil {
		return nil, err
	}
	return compiler.Compile(location)
})

// EncodeReplyIntent returns RFC 8785 JSON after strict schema and semantic checks.
func EncodeReplyIntent(event dto.ReplyIntent) ([]byte, error) {
	if !validStrings(reflect.ValueOf(event)) {
		return nil, invalidReply("invalid Unicode")
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return nil, invalidReply("encoding")
	}
	canonical, _, err := decodeReplyIntent(raw)
	return canonical, err
}

// DecodeReplyIntent rejects extra fields, duplicate keys, unsupported kinds,
// noncanonical deadlines, invalid Unicode and integers outside the exact range.
func DecodeReplyIntent(raw []byte) (dto.ReplyIntent, error) {
	_, event, err := decodeReplyIntent(raw)
	return event, err
}

// ReplyIntentDigest excludes no business fields. It is authorization evidence
// only after the Execution owner confirms this exact committed immutable digest.
func ReplyIntentDigest(event dto.ReplyIntent) (string, error) {
	canonical, err := EncodeReplyIntent(event)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
func decodeReplyIntent(raw []byte) ([]byte, dto.ReplyIntent, error) {
	var zero dto.ReplyIntent
	if len(raw) == 0 || len(raw) > MaxReplyIntentBytes || !validJSONUnicode(raw) {
		return nil, zero, invalidReply("size or Unicode")
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, zero, invalidReply("JSON syntax")
	}
	schema, err := compiledReplyIntentSchema()
	if err != nil {
		return nil, zero, errors.New("execution reply schema unavailable")
	}
	if err = schema.Validate(value); err != nil {
		return nil, zero, invalidReply("schema")
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, zero, invalidReply("noncanonical JSON")
	}
	var event dto.ReplyIntent
	if err = json.Unmarshal(canonical, &event); err != nil {
		return nil, zero, invalidReply("typed decoding")
	}
	if len(event.Content.Text) > MaxFinalTextBytes || strings.TrimSpace(event.Content.Text) == "" {
		return nil, zero, invalidReply("text boundary")
	}
	seen := map[string]map[int64]bool{}
	for _, a := range event.Content.Attachments {
		media, _, mimeErr := mime.ParseMediaType(a.MimeType)
		if mimeErr != nil || !strings.Contains(media, "/") {
			return nil, zero, invalidReply("attachment MIME")
		}
		if a.Name == "." || a.Name == ".." || len(a.Name) > 255 || strings.IndexFunc(a.Name, unicode.IsControl) >= 0 {
			return nil, zero, invalidReply("attachment name")
		}
		if seen[a.Name] == nil {
			seen[a.Name] = map[int64]bool{}
		}
		if seen[a.Name][a.Version] {
			return nil, zero, invalidReply("duplicate attachment")
		}
		seen[a.Name][a.Version] = true
	}
	deadline, err := time.Parse(time.RFC3339Nano, event.Deadline)
	if err != nil || deadline.IsZero() || deadline.Year() < 1 || deadline.UTC().Format(time.RFC3339Nano) != event.Deadline {
		return nil, zero, invalidReply("canonical deadline")
	}
	return canonical, event, nil
}
func invalidReply(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidReplyIntent, reason) }
