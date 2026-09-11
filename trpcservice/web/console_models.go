package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/node"
)

func (c *consoleAPI) systemStatus(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	if !requireSystemAdmin(writer, request) {
		return
	}
	statuses := make(map[string]string, len(c.dependencies.Probes))
	for name, probe := range c.dependencies.Probes {
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		err := probe(ctx)
		cancel()
		if err != nil {
			statuses[name] = fmt.Sprintf("error: %v", err)
			continue
		}
		statuses[name] = "ok"
	}
	nodes := []node.Record{}
	if c.dependencies.Nodes != nil {
		listed, err := c.dependencies.Nodes.List(request.Context())
		if err != nil {
			serverError(writer, "list service nodes", err)
			return
		}
		nodes = listed
	}
	c.modelMu.Lock()
	defer c.modelMu.Unlock()
	writeJSON(writer, http.StatusOK, map[string]any{"info": c.dependencies.System, "status": statuses, "nodes": nodes})
}

func (c *consoleAPI) models(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		methodNotAllowed(writer, http.MethodDelete)
		return
	}
	if !requireSystemAdmin(writer, request) {
		return
	}
	providerID := strings.TrimSpace(request.URL.Query().Get("provider_id"))
	modelName := strings.TrimSpace(request.URL.Query().Get("model"))
	if providerID == "" || modelName == "" {
		badRequest(writer, "provider_id and model are required")
		return
	}

	c.modelMu.Lock()
	providerIndex, modelIndex := -1, -1
	var target ModelInfo
	for i, provider := range c.dependencies.System.ModelProviders {
		if provider.ID != providerID {
			continue
		}
		providerIndex = i
		for j, modelInfo := range provider.Models {
			if modelInfo.Name == modelName {
				modelIndex = j
				target = modelInfo
				break
			}
		}
		break
	}
	if providerIndex < 0 || modelIndex < 0 {
		c.modelMu.Unlock()
		notFound(writer, "model does not exist")
		return
	}
	if target.Source != "discovered" {
		c.modelMu.Unlock()
		conflict(writer, "configured models must be removed from the platform configuration")
		return
	}
	c.modelMu.Unlock()

	applications, err := c.dependencies.Configurations.ListApplications(request.Context(), "")
	if err != nil {
		serverError(writer, "list applications before model removal", err)
		return
	}
	for _, application := range applications {
		modelConfig := application.Config.Model
		if modelConfig.ProviderID == providerID && modelConfig.Name == modelName {
			conflict(writer, "model is still used by application "+application.Config.AppName())
			return
		}
		for _, candidate := range modelConfig.FailoverCandidates {
			if candidate.ProviderID == providerID && candidate.Name == modelName {
				conflict(writer, "model is still used as a fallback by application "+application.Config.AppName())
				return
			}
		}
	}
	if c.dependencies.ModelRemover == nil {
		serverError(writer, "remove model", errors.New("model remover is not configured"))
		return
	}
	if err := c.dependencies.ModelRemover(request.Context(), providerID, modelName); err != nil {
		serverError(writer, "remove model", err)
		return
	}

	c.modelMu.Lock()
	for i := range c.dependencies.System.ModelProviders {
		provider := &c.dependencies.System.ModelProviders[i]
		if provider.ID != providerID {
			continue
		}
		filtered := provider.Models[:0]
		for _, modelInfo := range provider.Models {
			if modelInfo.Name != modelName {
				filtered = append(filtered, modelInfo)
			}
		}
		provider.Models = filtered
		break
	}
	c.modelMu.Unlock()
	writer.WriteHeader(http.StatusNoContent)
}

func (c *consoleAPI) syncModels(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	if !requireSystemAdmin(writer, request) {
		return
	}
	if c.dependencies.ModelSyncer == nil {
		c.modelMu.Lock()
		defer c.modelMu.Unlock()
		writeJSON(writer, http.StatusOK, map[string]any{"model_providers": c.dependencies.System.ModelProviders})
		return
	}
	updated, err := c.dependencies.ModelSyncer(request.Context())
	if err != nil {
		serverError(writer, "sync models", err)
		return
	}
	c.modelMu.Lock()
	defer c.modelMu.Unlock()
	c.dependencies.System.ModelProviders = updated
	writeJSON(writer, http.StatusOK, map[string]any{"model_providers": updated})
}
