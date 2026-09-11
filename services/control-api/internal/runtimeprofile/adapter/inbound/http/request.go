package httpadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/gowebpki/jcs"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type createRuntimeProfileRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type updateRuntimeProfileRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
}

type profileDraftRevisionRequest struct {
	ExpectedRevision int64 `json:"expected_revision"`
}

func decodeStrictJSON(c *gin.Context, destination any, maxBytes int64) error {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errors.New("content type must be application/json")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	data, err := io.ReadAll(c.Request.Body)
	defer clear(data)
	if err != nil || !utf8.Valid(data) {
		return errors.New("request must contain bounded UTF-8 JSON")
	}
	// JSON's duplicate names and encoding/json's case-insensitive struct matching
	// must not turn an invalid request into a different accepted command.
	if _, err := jcs.Transform(data); err != nil {
		return errors.New("request must contain one unambiguous JSON object")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return errors.New("request must be an object")
	}
	typ := reflect.TypeOf(destination).Elem()
	fields := make(map[string]bool, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		fields[strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	for name, value := range object {
		if !fields[name] || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("request has an unknown or null field")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func parsePage(c *gin.Context) (application.Page, error) {
	page := application.Page{Limit: 20}
	if value := c.Query("offset"); value != "" {
		offset, err := strconv.Atoi(value)
		if err != nil || offset < 0 {
			return application.Page{}, errors.New("invalid offset")
		}
		page.Offset = offset
	}
	if value := c.Query("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 100 {
			return application.Page{}, errors.New("invalid limit")
		}
		page.Limit = limit
	}
	return page, nil
}

func parseRevisionNumber(c *gin.Context) (int64, error) {
	value, err := strconv.ParseInt(c.Param("revision_number"), 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New("invalid revision number")
	}
	return value, nil
}

func readCredentialBody(c *gin.Context) ([]byte, error) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, domain.ErrCredentialInput
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, int64(domain.MaxDocumentBytes))
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		clear(data)
		return nil, domain.ErrCredentialInput
	}
	return data, nil
}
