package httpadapter

import (
	"encoding/json"
	"errors"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

type page struct {
	Offset int
	Limit  int
}

func parsePage(c *gin.Context) (page, bool) {
	offset, err := parseNonNegative(c.Query("offset"), 0)
	if err != nil {
		writeError(c, stdhttp.StatusBadRequest, "INVALID_PAGINATION", "offset must be non-negative")
		return page{}, false
	}
	limit, err := parseNonNegative(c.Query("limit"), 20)
	if err != nil || limit == 0 || limit > 100 {
		writeError(c, stdhttp.StatusBadRequest, "INVALID_PAGINATION", "limit must be between 1 and 100")
		return page{}, false
	}
	return page{Offset: offset, Limit: limit}, true
}

func parseNonNegative(value string, fallback int) (int, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, errors.New("invalid non-negative integer")
	}
	return parsed, nil
}

func handleError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, identityapp.ErrInvalidUsername):
		writeError(c, stdhttp.StatusBadRequest, "INVALID_USERNAME", "username does not satisfy policy")
	case errors.Is(err, identityapp.ErrWeakPassword):
		writeError(c, stdhttp.StatusBadRequest, "WEAK_PASSWORD", "password does not satisfy policy")
	case errors.Is(err, identityapp.ErrUsernameTaken):
		writeError(c, stdhttp.StatusConflict, "USERNAME_TAKEN", "username is already in use")
	case errors.Is(err, identityapp.ErrAccountNotFound):
		writeError(c, stdhttp.StatusNotFound, "ACCOUNT_NOT_FOUND", "account was not found")
	case errors.Is(err, application.ErrOperatorNotFound):
		writeError(c, stdhttp.StatusNotFound, "OPERATOR_NOT_FOUND", "operator was not found")
	case errors.Is(err, application.ErrLastOperator):
		writeError(c, stdhttp.StatusConflict, "LAST_OPERATOR", "last operator cannot be revoked")
	case errors.Is(err, tenantapp.ErrInvalidTenant):
		writeError(c, stdhttp.StatusBadRequest, "INVALID_TENANT", "tenant input is invalid")
	case errors.Is(err, tenantapp.ErrTenantSlugTaken):
		writeError(c, stdhttp.StatusConflict, "TENANT_SLUG_TAKEN", "tenant slug is already in use")
	case errors.Is(err, tenantapp.ErrAccountUnavailable):
		writeError(c, stdhttp.StatusBadRequest, "ACCOUNT_UNAVAILABLE", "owner account is unavailable")
	default:
		writeInternal(c)
	}
}

func decodeStrictJSON(c *gin.Context, destination any) error {
	if !strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "application/json") {
		return errors.New("content type must be application/json")
	}
	c.Request.Body = stdhttp.MaxBytesReader(c.Writer, c.Request.Body, 8*1024)
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

type publicError struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, publicError{Error: errorBody{Code: code, Message: message}})
}

func writeInternal(c *gin.Context) {
	writeError(c, stdhttp.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
}
