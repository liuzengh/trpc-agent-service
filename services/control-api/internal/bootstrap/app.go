package bootstrap

import (
	"context"
	"fmt"
	finaldeploymentpg "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend"
	backendconfig "github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/adapter/outbound/configfile"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding"
	channelnats "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/nats"
	channelpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/postgres"
	channelapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment"
	deploymenthttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/inbound/runtimehttp"
	deploymentnats "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/nats"
	deploymentpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/postgres"
	deploymentapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	deploymentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/infra/httpserver"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runmanagement"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/usagepolicy"
	"github.com/nats-io/nats.go"
)

type serverLifecycle interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

// App owns the modules and process lifecycle assembled for Control API.
type App struct {
	routeRelay      *channelapp.RouteRelay
	manifestRelay   *deploymentapp.ManifestRelay
	manifestNATS    *nats.Conn
	runtimeServer   serverLifecycle
	natsConnection  *nats.Conn
	database        *pgxpool.Pool
	server          serverLifecycle
	internalServer  serverLifecycle
	shutdownTimeout time.Duration
	identity        *identity.Module
	admin           *admin.Module
	tenant          *tenant.Module
	agent           *agent.Module
	runtimeProfile  *runtimeprofile.Module
	runManagement   *runmanagement.Module
	deployment      *deployment.Module
	channelBinding  *channelbinding.Module
	usagePolicy     *usagepolicy.Module
}

// New creates shared infrastructure and composes every Control API module.
func New(ctx context.Context, config Config) (*App, error) {
	// Every replica must agree with the release-pinned contract before touching
	// the database or constructing a listener. A mismatched replica never serves.
	platformContract, err := checkedDeploymentPlatformContract(config)
	if err != nil {
		return nil, err
	}
	backendCatalog, err := backendconfig.Load(config.PlatformBackendCatalogFile, config.PlatformBackendCatalogSHA256)
	if err != nil {
		return nil, fmt.Errorf("load platform backend catalog: %w", err)
	}
	backendTargets, err := backendconfig.LoadRuntime(config.PlatformBackendTargetsFile, config.PlatformBackendTargetsSHA256, backendCatalog)
	if err != nil {
		return nil, fmt.Errorf("load platform backend targets: %w", err)
	}
	pool, err := openDatabase(ctx, config)
	if err != nil {
		return nil, err
	}

	router := gin.New()
	if err := router.SetTrustedProxies(nil); err != nil {
		pool.Close()
		return nil, fmt.Errorf("configure HTTP proxy trust: %w", err)
	}
	// Gin's default debug recovery dumps request headers, including Cookie.
	// Recovery remains silent until telemetry provides a structured redactor.
	router.Use(gin.RecoveryWithWriter(nil))
	router.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	identityModule, err := identity.NewModule(identity.Dependencies{
		DB:              pool,
		Routes:          router,
		SessionLifetime: config.SessionLifetime,
		CookieName:      config.SessionCookieName,
		CookieDomain:    config.SessionCookieDomain,
		CookieSecure:    config.SessionCookieSecure,
		CookieSameSite:  http.SameSiteLaxMode,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble identity: %w", err)
	}
	tenantModule, err := tenant.NewModule(tenant.Dependencies{
		DB:           pool,
		Routes:       router,
		Authenticate: identityModule.AuthenticationMiddleware(),
		Accounts:     activeAccountLookup{accounts: identityModule.Accounts},
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble tenant: %w", err)
	}
	var usagePolicyWorkloads []channelapp.WorkloadPrincipal
	if config.Channel != nil {
		usagePolicyWorkloads = config.Channel.principals()
	}
	usagePolicyModule, err := usagepolicy.NewModule(usagepolicy.Dependencies{
		DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(),
		Access: activeTenantMemberLookup{tenants: tenantModule.Service}, Workloads: usagePolicyWorkloads,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble tenant usage policy: %w", err)
	}
	if _, err := platformbackend.NewModule(platformbackend.Dependencies{Routes: router, Authenticate: identityModule.AuthenticationMiddleware(), TenantAccess: activeTenantMemberLookup{tenants: tenantModule.Service}, Catalog: backendCatalog}); err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble backend directory: %w", err)
	}
	if err := ensureInitialPlatformOperator(ctx, pool, config); err != nil {
		pool.Close()
		return nil, err
	}
	adminModule, err := admin.NewModule(admin.Dependencies{
		DB:           pool,
		Routes:       router,
		Authenticate: identityModule.AuthenticationMiddleware(),
		Accounts:     identityModule.Accounts,
		Tenants:      tenantModule.Service,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble admin: %w", err)
	}
	agentModule, err := agent.NewModule(agent.Dependencies{
		DB: pool, Routes: router,
		Authenticate: identityModule.AuthenticationMiddleware(),
		TenantAccess: activeTenantMemberLookup{tenants: tenantModule.Service},
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble agent: %w", err)
	}
	var executionVerifier profileapp.ExecutionAuthorizationVerifier
	var authenticateWorker gin.HandlerFunc
	var runtimeRouter *gin.Engine
	if config.Runtime != nil {
		executionVerifier, err = config.Runtime.executionVerifier()
		if err != nil {
			pool.Close()
			return nil, err
		}
		authenticateWorker = config.Runtime.authenticateWorker()
		runtimeRouter = gin.New()
		_ = runtimeRouter.SetTrustedProxies(nil)
		runtimeRouter.Use(gin.RecoveryWithWriter(nil))
	}
	var finalArtifacts profileapp.FinalArtifactAuthorizer
	if proofVerifier, ok := executionVerifier.(finalProofVerifier); ok {
		finalArtifacts = finalArtifactAuthorization{verifier: proofVerifier, manifests: finaldeploymentpg.NewStore(pool)}
	}
	runtimeProfileModule, err := runtimeprofile.NewModule(runtimeprofile.Dependencies{
		ManagedCredentialTargets: deploymentBackendAccess{targets: backendTargets, tenants: activeTenantMemberLookup{tenants: tenantModule.Service}},
		Backends:                 profileBackendAccess{catalog: backendCatalog},
		FinalArtifacts:           finalArtifacts, ExecutionVerifier: executionVerifier, AuthenticateWorker: authenticateWorker, RuntimeRoutes: runtimeRouter,
		DB: pool, Routes: router,
		Authenticate:  identityModule.AuthenticationMiddleware(),
		CredentialKey: config.ProfileCredentialKey,
		OwnerAccess:   activeTenantMemberLookup{tenants: tenantModule.Service},
		TenantAccess:  activeTenantMemberLookup{tenants: tenantModule.Service},
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble runtime profile: %w", err)
	}
	var runManagementModule *runmanagement.Module
	if config.Runtime != nil {
		managementClient, clientErr := config.Runtime.managementClient()
		if clientErr != nil {
			pool.Close()
			return nil, clientErr
		}
		runManagementModule, err = runmanagement.NewModule(runmanagement.Dependencies{DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(), TenantAccess: activeTenantMemberLookup{tenants: tenantModule.Service}, Runtime: managementClient})
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("assemble run management: %w", err)
		}
	}
	var knowledgeBackend deploymentapp.KnowledgeBackend
	if config.Runtime != nil {
		knowledgeBackend, err = config.Runtime.knowledgeClient()
		if err != nil {
			pool.Close()
			return nil, err
		}
	}
	var artifactBackend deploymentapp.ArtifactBackend
	if config.Runtime != nil {
		artifactBackend, err = config.Runtime.artifactClient()
		if err != nil {
			pool.Close()
			return nil, err
		}
	}
	var backendMigrations deploymentapp.BackendMigrator
	if config.Runtime != nil {
		backendMigrations, err = config.Runtime.backendMigrationClient()
		if err != nil {
			pool.Close()
			return nil, err
		}
	}
	deploymentModule, err := deployment.NewModule(deployment.Dependencies{
		ManagedBackends: deploymentBackendAccess{targets: backendTargets, tenants: activeTenantMemberLookup{tenants: tenantModule.Service}},
		ArtifactBackend: artifactBackend, ArtifactCredentials: runtimeProfileModule.Service,
		KnowledgeBackend: knowledgeBackend, KnowledgeCredentials: runtimeProfileModule.Service,
		DB: pool, Routes: router,
		Authenticate:       identityModule.AuthenticationMiddleware(),
		TenantAccess:       activeTenantMemberLookup{tenants: tenantModule.Service},
		AgentVersions:      agentModule.Service,
		ProfileRevisions:   runtimeProfileModule.Service,
		ProfileCredentials: runtimeProfileModule.Service,
		StorageCredentials: runtimeProfileModule.Service,
		BackendMigrations:  backendMigrations,
		Platform:           platformContract,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("assemble deployment: %w", err)
	}

	var channelModule *channelbinding.Module
	var internalServer serverLifecycle
	var nc *nats.Conn
	var relay *channelapp.RouteRelay
	if config.Channel != nil {
		tlsConfig, err := config.Channel.tlsConfig()
		if err != nil {
			pool.Close()
			return nil, err
		}
		channelModule, err = channelbinding.NewModule(channelbinding.Dependencies{
			DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(),
			TenantAccess:          channelTenantAccess{members: activeTenantMemberLookup{tenants: tenantModule.Service}},
			TransactionAuthorizer: channelTransactionAuthorizer{}, Deployments: deploymentModule.Service,
			Cipher: config.Channel.cipher, Options: channelpostgres.Options{ScopeID: config.Channel.ScopeID, SourceEpoch: config.Channel.SourceEpoch, MaxTenantAccounts: config.Channel.MaxTenantAccounts}, Workloads: config.Channel.principals(),
		})
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("assemble channel: %w", err)
		}
		if err = channelModule.Initialize(ctx); err != nil {
			pool.Close()
			return nil, fmt.Errorf("initialize channel: %w", err)
		}
		nc, err = config.Channel.connectNATS()
		if err != nil {
			pool.Close()
			return nil, err
		}
		publisher, err := channelnats.NewPublisher(nc)
		if err != nil {
			nc.Close()
			pool.Close()
			return nil, err
		}
		relay, err = channelModule.NewRouteRelay(publisher)
		if err != nil {
			nc.Close()
			pool.Close()
			return nil, err
		}
		internalMux := http.NewServeMux()
		internalMux.Handle("GET /internal/v1/tenants/{tenant_id}/usage-policy", usagePolicyModule.InternalHandler)
		internalMux.Handle("/", channelModule.InternalHandler)
		internalServer = httpserver.NewTLS(config.Channel.InternalAddress, internalMux, tlsConfig)
	}

	var runtimeServer serverLifecycle
	var manifestRelay *deploymentapp.ManifestRelay
	var manifestNATS *nats.Conn
	if config.Runtime != nil {
		tc, tlsErr := config.Runtime.serverTLS()
		if tlsErr != nil {
			if nc != nil {
				nc.Close()
			}
			pool.Close()
			return nil, tlsErr
		}
		manifestNATS, err = config.Runtime.connectNATS()
		if err != nil {
			if nc != nil {
				nc.Close()
			}
			pool.Close()
			return nil, err
		}
		publisher, publishErr := deploymentnats.NewManifestPublisher(manifestNATS)
		if publishErr != nil {
			manifestNATS.Close()
			if nc != nil {
				nc.Close()
			}
			pool.Close()
			return nil, publishErr
		}
		store := deploymentpostgres.NewStore(pool)
		manifestRelay, err = deploymentapp.NewManifestRelay(store, publisher)
		if err != nil {
			manifestNATS.Close()
			if nc != nil {
				nc.Close()
			}
			pool.Close()
			return nil, err
		}
		deploymenthttp.RegisterManifestExport(runtimeRouter.Group("", authenticateWorker), store, config.ProfileCredentialKey)
		runtimeServer = httpserver.NewTLS(config.Runtime.InternalAddress, runtimeRouter, tc)
	}
	return &App{
		routeRelay: relay, natsConnection: nc,
		manifestRelay: manifestRelay, manifestNATS: manifestNATS, runtimeServer: runtimeServer,
		database:        pool,
		server:          httpserver.New(config.HTTPAddress, router),
		shutdownTimeout: config.ShutdownTimeout,
		identity:        identityModule,
		admin:           adminModule,
		tenant:          tenantModule,
		agent:           agentModule,
		runtimeProfile:  runtimeProfileModule,
		runManagement:   runManagementModule,
		deployment:      deploymentModule,
		channelBinding:  channelModule,
		usagePolicy:     usagePolicyModule,
		internalServer:  internalServer,
	}, nil
}

func deploymentPlatformContract(config Config) (deploymentdomain.PlatformExecutionContract, error) {
	contract := deploymentdomain.WorkerV1PlatformExecutionContract()
	if err := bindManagedCatalogDigest(config, &contract); err != nil {
		return deploymentdomain.PlatformExecutionContract{}, err
	}
	digest, err := contract.CalculateDigest()
	contract.Digest = digest
	if err != nil {
		return deploymentdomain.PlatformExecutionContract{}, err
	}
	if err := contract.Validate(); err != nil {
		return deploymentdomain.PlatformExecutionContract{}, fmt.Errorf(
			"validate deployment platform contract: %w", err,
		)
	}
	return contract, nil
}
