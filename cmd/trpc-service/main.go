package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("trpc-service", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	showVersion := flags.Bool("version", false, "print version and exit")
	migrateOnly := flags.Bool("migrate-only", false, "apply PostgreSQL migrations and exit")
	configPath := flags.String("config", "", "validated tenant config for the gateway")
	listenAddress := flags.String("listen", "127.0.0.1:8080", "OpenClaw HTTP listen address")
	roleName := flags.String("role", string(roleAll), "process role: all, gateway, or worker")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "unexpected arguments: %v\n", flags.Args())
		return 2
	}
	if *showVersion {
		fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
		return 0
	}
	role, err := parseProcessRole(*roleName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize service: %v\n", err)
		return 2
	}
	if err := configureExternalSecretProviders(); err != nil {
		fmt.Fprintf(os.Stderr, "initialize service: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if *migrateOnly {
		if err := migrateSchema(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "migrate database: %v\n", err)
			return 1
		}
		fmt.Println("database migrations applied")
		return 0
	}

	var options []trpcservice.Option
	if *configPath != "" {
		component, err := gatewayComponent(ctx, *configPath, *listenAddress, role)
		if err != nil {
			fmt.Fprintf(os.Stderr, "initialize gateway: %v\n", err)
			return 1
		}
		options = append(options, trpcservice.WithComponents(component))
	}
	shutdownTimeout, err := positiveEnvDuration(shutdownTimeoutEnv, 10*time.Second, time.Second, 10*time.Minute)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize service: %v\n", err)
		return 1
	}
	options = append(options, trpcservice.WithShutdownTimeout(shutdownTimeout))
	app, err := trpcservice.NewApp(options...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize service: %v\n", err)
		return 1
	}

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	if err := app.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "service stopped with error: %v\n", err)
		return 1
	}
	return 0
}

func configureExternalSecretProviders() error {
	for _, configured := range []struct {
		provider tenant.SecretProvider
		mode     secret.HTTPProviderMode
		endpoint string
		token    string
	}{
		{tenant.SecretProviderVault, secret.HTTPProviderVault, os.Getenv("TRPC_AGENT_VAULT_ENDPOINT"), os.Getenv("TRPC_AGENT_VAULT_TOKEN")},
		{tenant.SecretProviderKMS, secret.HTTPProviderKMS, os.Getenv("TRPC_AGENT_KMS_ENDPOINT"), os.Getenv("TRPC_AGENT_KMS_TOKEN")},
	} {
		if configured.endpoint == "" && configured.token == "" {
			continue
		}
		if configured.endpoint == "" || configured.token == "" {
			return errors.New("external secret provider endpoint and token must be configured together")
		}
		if err := secret.RegisterProvider(configured.provider, &secret.HTTPProvider{Endpoint: configured.endpoint, Token: configured.token, Mode: configured.mode}); err != nil {
			return errors.New("external secret provider registration failed")
		}
	}
	return nil
}

// gatewayComponent seeds the control plane from the startup file and wires the
// production component. Inbound routing, WeCom callback bindings, and runtime
// snapshots and outbound senders all resolve from the control-plane database
// at request time, so Admin API publishes take effect without restarting the
// service.
func gatewayComponent(ctx context.Context, path, address string, role processRole) (trpcservice.Component, error) {
	fileHandle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fileHandle.Close()
	file, err := config.Load(fileHandle)
	if err != nil {
		return nil, err
	}
	if err := file.ValidateProduction(); err != nil {
		return nil, err
	}
	return newDurableComponent(ctx, address, file, role)
}

func resolveLocalSecret(ref tenant.SecretRef) (string, error) {
	return secret.Resolve(context.Background(), ref)
}

func gatewayTokenEnv(bindingID string) string {
	var name strings.Builder
	name.WriteString("TRPC_AGENT_GATEWAY_TOKEN_")
	for _, char := range bindingID {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			name.WriteRune(unicode.ToUpper(char))
		} else {
			name.WriteByte('_')
		}
	}
	return name.String()
}
