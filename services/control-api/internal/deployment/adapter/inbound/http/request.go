package httpadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/gowebpki/jcs"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

const maxDeploymentRequestBytes int64 = 32 * 1024

var (
	errInvalidRequestBody = errors.New("invalid deployment request body")
	errRequestTooLarge    = errors.New("deployment request body too large")
)

type createDeploymentRequest struct {
	Name        string
	Description string
}

type updateDeploymentRequest struct {
	ExpectedMetadataRevision int64
	Name                     *string
	Description              *string
}

type publishDeploymentRequest struct {
	ExpectedLatestRevisionNumber *int64
	Input                        domain.DeploymentInput
}

type migrateAndPublishRequest struct {
	SourceRevisionNumber         int64
	ExpectedLatestRevisionNumber *int64
	Input                        domain.DeploymentInput
}

func decodeCreateDeploymentRequest(c *gin.Context) (createDeploymentRequest, error) {
	object, err := readStrictJSONObject(c)
	if err != nil {
		return createDeploymentRequest{}, err
	}
	if err := validateObjectFields(object, []string{"name"}, []string{"description"}, nil); err != nil {
		return createDeploymentRequest{}, err
	}
	name, err := decodeString(object["name"])
	if err != nil {
		return createDeploymentRequest{}, errInvalidRequestBody
	}
	description := ""
	if raw, ok := object["description"]; ok {
		description, err = decodeString(raw)
		if err != nil {
			return createDeploymentRequest{}, errInvalidRequestBody
		}
	}
	if !validDeploymentName(name) || !validDeploymentDescription(description) {
		return createDeploymentRequest{}, errInvalidRequestBody
	}
	return createDeploymentRequest{Name: name, Description: description}, nil
}

func decodeUpdateDeploymentRequest(c *gin.Context) (updateDeploymentRequest, error) {
	object, err := readStrictJSONObject(c)
	if err != nil {
		return updateDeploymentRequest{}, err
	}
	if err := validateObjectFields(
		object,
		[]string{"expected_metadata_revision"},
		[]string{"name", "description"},
		nil,
	); err != nil {
		return updateDeploymentRequest{}, err
	}
	expected, err := decodePositiveInt64(object["expected_metadata_revision"])
	if err != nil {
		return updateDeploymentRequest{}, errInvalidRequestBody
	}
	result := updateDeploymentRequest{ExpectedMetadataRevision: expected}
	if raw, ok := object["name"]; ok {
		value, err := decodeString(raw)
		if err != nil || !validDeploymentName(value) {
			return updateDeploymentRequest{}, errInvalidRequestBody
		}
		result.Name = &value
	}
	if raw, ok := object["description"]; ok {
		value, err := decodeString(raw)
		if err != nil || !validDeploymentDescription(value) {
			return updateDeploymentRequest{}, errInvalidRequestBody
		}
		result.Description = &value
	}
	if result.Name == nil && result.Description == nil {
		return updateDeploymentRequest{}, errInvalidRequestBody
	}
	return result, nil
}

func decodeDeploymentInputRequest(c *gin.Context) (domain.DeploymentInput, error) {
	object, err := readStrictJSONObject(c)
	if err != nil {
		return domain.DeploymentInput{}, err
	}
	return decodeDeploymentInputObject(object)
}

func decodePublishDeploymentRequest(c *gin.Context) (publishDeploymentRequest, error) {
	object, err := readStrictJSONObject(c)
	if err != nil {
		return publishDeploymentRequest{}, err
	}
	if err := validateObjectFields(
		object,
		[]string{"expected_latest_revision_number", "input"},
		nil,
		map[string]bool{"expected_latest_revision_number": true},
	); err != nil {
		return publishDeploymentRequest{}, err
	}
	var expected *int64
	if raw := object["expected_latest_revision_number"]; !isJSONNull(raw) {
		value, err := decodePositiveInt64(raw)
		if err != nil {
			return publishDeploymentRequest{}, errInvalidRequestBody
		}
		expected = &value
	}
	inputObject, err := decodeJSONObject(object["input"])
	if err != nil {
		return publishDeploymentRequest{}, errInvalidRequestBody
	}
	input, err := decodeDeploymentInputObject(inputObject)
	if err != nil {
		return publishDeploymentRequest{}, err
	}
	return publishDeploymentRequest{
		ExpectedLatestRevisionNumber: expected,
		Input:                        input,
	}, nil
}

func decodeMigrateAndPublishRequest(c *gin.Context) (migrateAndPublishRequest, error) {
	object, err := readStrictJSONObject(c)
	if err != nil {
		return migrateAndPublishRequest{}, err
	}
	if err := validateObjectFields(object, []string{"source_revision_number", "expected_latest_revision_number", "input"}, nil, map[string]bool{"expected_latest_revision_number": true}); err != nil {
		return migrateAndPublishRequest{}, err
	}
	source, err := decodePositiveInt64(object["source_revision_number"])
	if err != nil {
		return migrateAndPublishRequest{}, errInvalidRequestBody
	}
	var expected *int64
	if raw := object["expected_latest_revision_number"]; !isJSONNull(raw) {
		value, err := decodePositiveInt64(raw)
		if err != nil {
			return migrateAndPublishRequest{}, errInvalidRequestBody
		}
		expected = &value
	}
	inputObject, err := decodeJSONObject(object["input"])
	if err != nil {
		return migrateAndPublishRequest{}, errInvalidRequestBody
	}
	input, err := decodeDeploymentInputObject(inputObject)
	if err != nil {
		return migrateAndPublishRequest{}, err
	}
	return migrateAndPublishRequest{SourceRevisionNumber: source, ExpectedLatestRevisionNumber: expected, Input: input}, nil
}

func readStrictJSONObject(c *gin.Context) (map[string]json.RawMessage, error) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, errInvalidRequestBody
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxDeploymentRequestBytes)
	data, err := io.ReadAll(c.Request.Body)
	defer clear(data)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return nil, errRequestTooLarge
		}
		return nil, errInvalidRequestBody
	}
	if len(bytes.TrimSpace(data)) == 0 || !utf8.Valid(data) {
		return nil, errInvalidRequestBody
	}
	// JCS rejects duplicate object names and ambiguous/trailing JSON before Go's
	// case-insensitive struct matching can alter command meaning.
	if _, err := jcs.Transform(data); err != nil {
		return nil, errInvalidRequestBody
	}
	return decodeJSONObject(data)
}

func decodeJSONObject(data []byte) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return nil, errInvalidRequestBody
	}
	return object, nil
}

func decodeDeploymentInputObject(object map[string]json.RawMessage) (domain.DeploymentInput, error) {
	if err := validateObjectFields(object, []string{"schema_version", "agent", "profile"}, nil, nil); err != nil {
		return domain.DeploymentInput{}, err
	}
	schemaVersion, err := decodeString(object["schema_version"])
	if err != nil || schemaVersion != domain.SchemaVersionV1 {
		return domain.DeploymentInput{}, errInvalidRequestBody
	}
	agentObject, err := decodeJSONObject(object["agent"])
	if err != nil {
		return domain.DeploymentInput{}, errInvalidRequestBody
	}
	if err := validateObjectFields(agentObject, []string{"agent_id", "version_number"}, nil, nil); err != nil {
		return domain.DeploymentInput{}, err
	}
	agentID, err := decodeString(agentObject["agent_id"])
	if err != nil || !validOpaqueID(agentID) {
		return domain.DeploymentInput{}, errInvalidRequestBody
	}
	agentVersion, err := decodePositiveInt64(agentObject["version_number"])
	if err != nil {
		return domain.DeploymentInput{}, errInvalidRequestBody
	}
	profileObject, err := decodeJSONObject(object["profile"])
	if err != nil {
		return domain.DeploymentInput{}, errInvalidRequestBody
	}
	if err := validateObjectFields(profileObject, []string{"profile_id", "revision_number"}, nil, nil); err != nil {
		return domain.DeploymentInput{}, err
	}
	profileID, err := decodeString(profileObject["profile_id"])
	if err != nil || !validOpaqueID(profileID) {
		return domain.DeploymentInput{}, errInvalidRequestBody
	}
	profileRevision, err := decodePositiveInt64(profileObject["revision_number"])
	if err != nil {
		return domain.DeploymentInput{}, errInvalidRequestBody
	}
	return domain.DeploymentInput{
		SchemaVersion: schemaVersion,
		Agent: domain.AgentInput{
			AgentID: agentID, VersionNumber: agentVersion,
		},
		Profile: domain.ProfileInput{
			ProfileID: profileID, RevisionNumber: profileRevision,
		},
	}, nil
}

func validateObjectFields(
	object map[string]json.RawMessage,
	required, optional []string,
	nullable map[string]bool,
) error {
	allowed := make(map[string]bool, len(required)+len(optional))
	for _, name := range required {
		allowed[name] = true
		if _, ok := object[name]; !ok {
			return errInvalidRequestBody
		}
	}
	for _, name := range optional {
		allowed[name] = true
	}
	for name, value := range object {
		if !allowed[name] || (isJSONNull(value) && !nullable[name]) {
			return errInvalidRequestBody
		}
	}
	return nil
}

func isJSONNull(value []byte) bool {
	return bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func decodeString(raw []byte) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodePositiveInt64(raw []byte) (int64, error) {
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil || value <= 0 {
		return 0, errInvalidRequestBody
	}
	return value, nil
}

func parsePage(c *gin.Context) (application.Page, error) {
	query := c.Request.URL.Query()
	for name, values := range query {
		if (name != "offset" && name != "limit") || len(values) != 1 || values[0] == "" {
			return application.Page{}, errInvalidRequestBody
		}
	}
	page := application.Page{Limit: 20}
	if values, ok := query["offset"]; ok {
		offset, err := strconv.Atoi(values[0])
		if err != nil || offset < 0 {
			return application.Page{}, errInvalidRequestBody
		}
		page.Offset = offset
	}
	if values, ok := query["limit"]; ok {
		limit, err := strconv.Atoi(values[0])
		if err != nil || limit < 1 || limit > 100 {
			return application.Page{}, errInvalidRequestBody
		}
		page.Limit = limit
	}
	return page, nil
}

func parseRevisionNumber(c *gin.Context) (int64, error) {
	value, err := strconv.ParseInt(c.Param("revision_number"), 10, 64)
	if err != nil || value <= 0 {
		return 0, errInvalidRequestBody
	}
	return value, nil
}

func validRouteIDs(c *gin.Context) bool {
	if !validOpaqueID(c.Param("tenant_id")) {
		return false
	}
	if value := c.Param("deployment_id"); value != "" && !validOpaqueID(value) {
		return false
	}
	return true
}

func validOpaqueID(value string) bool {
	return value != "" && utf8.RuneCountInString(value) <= 128
}

func validDeploymentName(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" && utf8.RuneCountInString(value) <= 128
}

func validDeploymentDescription(value string) bool {
	return utf8.RuneCountInString(value) <= 4096
}

func validIdempotencyKey(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 33 || character > 126 {
			return false
		}
	}
	return true
}

func idempotencyKey(c *gin.Context) (string, bool) {
	values := c.Request.Header.Values("Idempotency-Key")
	if len(values) != 1 || !validIdempotencyKey(values[0]) {
		return "", false
	}
	return values[0], true
}
