package httpadapter

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

type createAgentRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type updateAgentRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
}

type saveDraftRequest struct {
	ExpectedRevision int64           `json:"expected_revision"`
	Spec             json.RawMessage `json:"spec"`
}

type revisionRequest struct {
	ExpectedRevision int64 `json:"expected_revision"`
}

func decodeStrictJSON(c *gin.Context, destination any, maxBytes int64) error {
	if !strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "application/json") {
		return errors.New("content type must be application/json")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request must contain exactly one JSON value")
	}
	return nil
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

func parseVersionNumber(c *gin.Context) (int64, error) {
	value, err := strconv.ParseInt(c.Param("version_number"), 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New("invalid version number")
	}
	return value, nil
}

const maxAgentSpecRequestBytes = domain.MaxDocumentBytes + 8*1024
