//go:build integration

package milvus

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/milvus-io/milvus/client/v2/entity"
	sdk "github.com/milvus-io/milvus/client/v2/milvusclient"
)

// ProvisionTestCollection provisions the server-owned collection, index and
// load state for integration fixtures. It is only compiled with the
// integration build tag and is never referenced by production code paths.
func ProvisionTestCollection(ctx context.Context, cfg vector.BackendConfig, address string, credentials Credentials) error {
	client, err := sdk.New(ctx, &sdk.ClientConfig{Address: address, Username: credentials.Username, Password: credentials.Password, APIKey: credentials.APIKey})
	if err != nil {
		return err
	}
	defer client.Close(ctx)
	provisionCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	createOption := sdk.NewCreateCollectionOption(cfg.Collection, expectedSchema(cfg)).WithConsistencyLevel(entity.ClStrong)
	for key, value := range expectedProperties(cfg) {
		createOption = createOption.WithProperty(key, value)
	}
	if err := client.CreateCollection(provisionCtx, createOption); err != nil {
		return err
	}
	task, err := client.CreateIndex(provisionCtx, sdk.NewCreateIndexOption(cfg.Collection, fieldVector, expectedIndex(cfg)).WithIndexName(indexName))
	if err != nil {
		return err
	}
	if err := task.Await(provisionCtx); err != nil {
		return err
	}
	loadTask, err := client.LoadCollection(provisionCtx, sdk.NewLoadCollectionOption(cfg.Collection))
	if err != nil {
		return err
	}
	return loadTask.Await(provisionCtx)
}
