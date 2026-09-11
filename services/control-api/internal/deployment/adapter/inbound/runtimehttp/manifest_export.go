package runtimehttp

import (
	"errors"
	"github.com/gin-gonic/gin"
	controlruntimev1 "github.com/liuzengh/trpc-agent-service/api/runtime/control/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"net/http"
	"strconv"
)

// RegisterManifestExport requires a group protected by the release-pinned mTLS
// Worker mapping. No tenant or manifest identity is taken as authentication.
func RegisterManifestExport(routes gin.IRoutes, store application.ManifestExportStore, cursorKey []byte) {
	routes.GET(controlruntimev1.ManifestExportPath, func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		values := c.Request.URL.Query()
		for k, v := range values {
			if (k != "cursor" && k != "limit") || len(v) != 1 {
				c.Status(http.StatusBadRequest)
				return
			}
		}
		limit := controlruntimev1.MaxExportPageSize
		if value := values.Get("limit"); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil {
				c.Status(http.StatusBadRequest)
				return
			}
			limit = n
		}
		page, err := application.ExportManifestPage(c.Request.Context(), store, values.Get("cursor"), limit, cursorKey)
		if errors.Is(err, application.ErrManifestExportCursor) {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "INVALID_EXPORT_CURSOR"}})
			return
		}
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"code": "MANIFEST_EXPORT_UNAVAILABLE"}})
			return
		}
		c.JSON(http.StatusOK, page)
	})
}
