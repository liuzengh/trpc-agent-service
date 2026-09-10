package postgres

import (
	"database/sql"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	agentpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/backend/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

// The cross-domain suite exercises the public domain repositories through the
// same shared storage primitives used by bootstrap. These aliases exist only
// in tests and do not add a production storage-to-domain dependency.
type AppRepository = agentpostgres.AppRepository
type AgentRepository = AppRepository
type BackendRepository = backendpostgres.BackendRepository
type ChannelRepository = channelpostgres.ChannelRepository
type ModelRepository = modelpostgres.ModelRepository
type TenantRepository = tenantpostgres.TenantRepository

var ErrStorage = storagepostgres.ErrStorage

func NewAppRepository(db *sql.DB) *AppRepository {
	return agentpostgres.NewAppRepository(db)
}

func NewAgentRepository(db *sql.DB) *AgentRepository {
	return NewAppRepository(db)
}

func NewBackendRepository(db *sql.DB, catalog *backend.ProviderCatalog) *BackendRepository {
	return backendpostgres.NewRepository(db, catalog)
}

func NewChannelRepository(db *sql.DB) *ChannelRepository {
	return channelpostgres.NewRepository(db)
}

func NewModelRepository(db *sql.DB, catalog *model.ProviderCatalog) *ModelRepository {
	return modelpostgres.NewRepository(db, catalog)
}

func NewTenantRepository(db *sql.DB) *TenantRepository {
	return tenantpostgres.NewRepository(db)
}

func encodeAgentRevisionParts(revision appmodel.Revision) ([]byte, []byte, []byte, error) {
	generation, err := storagepostgres.EncodeJSON(revision.Generation)
	if err != nil {
		return nil, nil, nil, err
	}
	runtime, err := storagepostgres.EncodeJSON(revision.Runtime)
	if err != nil {
		return nil, nil, nil, err
	}
	tools, err := storagepostgres.EncodeJSON(revision.Tools)
	if err != nil {
		return nil, nil, nil, err
	}
	return generation, runtime, tools, nil
}

func encodeModelJSON(configuration model.Configuration) ([]byte, []byte, error) {
	options, err := storagepostgres.EncodeJSON(configuration.Options)
	if err != nil {
		return nil, nil, err
	}
	generation, err := storagepostgres.EncodeJSON(configuration.Generation)
	if err != nil {
		return nil, nil, err
	}
	return options, generation, nil
}

func encodeProtocol(protocol channels.ProtocolConfiguration) ([]byte, error) {
	return storagepostgres.EncodeJSON(protocol)
}
