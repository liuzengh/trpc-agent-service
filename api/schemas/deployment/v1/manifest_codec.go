package deploymentv1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const MaxManifestContentBytes = 512 * 1024
const MaxManifestBytes = 1024 * 1024

var ErrInvalidManifest = errors.New("invalid runtime manifest")

type RuntimeManifest struct {
	ID                   string          `json:"manifest_id"`
	TenantID             string          `json:"tenant_id"`
	DeploymentID         string          `json:"deployment_id"`
	DeploymentRevisionID string          `json:"deployment_revision_id"`
	RevisionNumber       int64           `json:"revision_number"`
	Content              json.RawMessage `json:"content"`
	ContentDigest        string          `json:"content_digest"`
	PublishedAt          time.Time       `json:"published_at"`
}

type validatorPair struct{ full, content *jsonschema.Schema }

var manifestValidators = sync.OnceValues(func() (validatorPair, error) {
	var document any
	if err := json.Unmarshal(ManifestSchema, &document); err != nil {
		return validatorPair{}, err
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	const location = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest.schema.json"
	if err := c.AddResource(location, document); err != nil {
		return validatorPair{}, err
	}
	full, err := c.Compile(location)
	if err != nil {
		return validatorPair{}, err
	}
	content, err := c.Compile(location + "#/$defs/content")
	return validatorPair{full, content}, err
})

func validateDocument(raw []byte, contentOnly bool) ([]byte, error) {
	limit := MaxManifestBytes
	if contentOnly {
		limit = MaxManifestContentBytes
	}
	if len(raw) == 0 || len(raw) > limit || !utf8.Valid(raw) {
		return nil, ErrInvalidManifest
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, ErrInvalidManifest
	}
	validators, err := manifestValidators()
	if err != nil {
		return nil, fmt.Errorf("compile manifest schema: %w", err)
	}
	validator := validators.full
	if contentOnly {
		validator = validators.content
	}
	if err = validator.Validate(doc); err != nil {
		return nil, ErrInvalidManifest
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, ErrInvalidManifest
	}
	return canonical, nil
}
func strictDecodeJSON(raw []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return ErrInvalidManifest
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrInvalidManifest
	}
	return nil
}
func DecodeManifestContent(raw []byte) (ManifestContent, error) {
	var out ManifestContent
	canonical, err := validateDocument(raw, true)
	if err != nil {
		return out, err
	}
	if err = strictDecodeJSON(canonical, &out); err != nil {
		return ManifestContent{}, err
	}
	if err := validateManagedResourceRoles(out); err != nil {
		return ManifestContent{}, err
	}
	return out, nil
}
func DecodeRuntimeManifest(raw []byte) (RuntimeManifest, error) {
	var out RuntimeManifest
	canonical, err := validateDocument(raw, false)
	if err != nil {
		return out, err
	}
	if err = strictDecodeJSON(canonical, &out); err != nil {
		return RuntimeManifest{}, err
	}
	if _, err = VerifyRuntimeManifest(out); err != nil {
		return RuntimeManifest{}, err
	}
	return out, nil
}

// VerifyRuntimeManifest validates the full envelope and independently recomputes
// the RFC 8785 digest. It never substitutes a redacted view or mutable sources.
func VerifyRuntimeManifest(manifest RuntimeManifest) (ManifestContent, error) {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return ManifestContent{}, ErrInvalidManifest
	}
	if _, err = validateDocument(raw, false); err != nil {
		return ManifestContent{}, err
	}
	content, err := DecodeManifestContent(manifest.Content)
	if err != nil {
		return ManifestContent{}, err
	}
	canonical, err := jcs.Transform(manifest.Content)
	if err != nil {
		return ManifestContent{}, ErrInvalidManifest
	}
	hash := sha256.Sum256(canonical)
	if manifest.ContentDigest != "sha256:"+hex.EncodeToString(hash[:]) || content.TenantID != manifest.TenantID {
		return ManifestContent{}, ErrInvalidManifest
	}
	return content, nil
}
