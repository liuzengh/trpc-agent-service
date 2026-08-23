package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

type SmokeResult struct {
	TenantID       string `json:"tenant_id"`
	QdrantIsolated bool   `json:"qdrant_isolated"`
	MinIOIsolated  bool   `json:"minio_isolated"`
}

func Smoke(ctx context.Context, router *Router, tenants []tenant.Tenant) ([]SmokeResult, error) {
	if router == nil || len(tenants) == 0 {
		return nil, fmt.Errorf("router and at least one tenant are required")
	}
	logicalKey := "smoke/shared-key.txt"
	vector := []float32{1, 0, 0, 0}
	type sentinel struct {
		tenantID string
		pointID  string
		content  string
	}
	items := make([]sentinel, 0, len(tenants))
	for _, profile := range tenants {
		item := sentinel{tenantID: profile.ID, pointID: uuid.NewString(), content: "artifact:" + profile.ID}
		if err := router.Upsert(ctx, profile.ID, KnowledgeItem{ID: item.pointID, Vector: vector, Payload: map[string]any{"kind": "smoke"}}); err != nil {
			return nil, err
		}
		if err := router.Put(ctx, profile.ID, logicalKey, strings.NewReader(item.content)); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	defer func() {
		for _, item := range items {
			_ = router.Delete(context.Background(), item.tenantID, item.pointID)
			_ = router.DeleteArtifact(context.Background(), item.tenantID, logicalKey)
		}
	}()

	results := make([]SmokeResult, 0, len(items))
	for _, item := range items {
		points, err := router.Search(ctx, item.tenantID, vector)
		if err != nil {
			return nil, err
		}
		qdrantIsolated := false
		for _, point := range points {
			if point.ID == item.pointID {
				qdrantIsolated = true
			}
			for _, other := range items {
				if other.tenantID != item.tenantID && point.ID == other.pointID {
					return nil, fmt.Errorf("qdrant tenant %s read tenant %s point", item.tenantID, other.tenantID)
				}
			}
		}
		body, err := router.Get(ctx, item.tenantID, logicalKey)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(io.LimitReader(body, 1024))
		body.Close()
		if readErr != nil {
			return nil, readErr
		}
		minioIsolated := bytes.Equal(payload, []byte(item.content))
		if !qdrantIsolated || !minioIsolated {
			return nil, fmt.Errorf("backend smoke failed for tenant %s", item.tenantID)
		}
		results = append(results, SmokeResult{TenantID: item.tenantID, QdrantIsolated: true, MinIOIsolated: true})
	}
	return results, nil
}
