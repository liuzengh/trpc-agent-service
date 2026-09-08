// trpc-embeddingcheck makes one explicit, non-business embedding request. It
// never starts an Agent, reads conversations, ingests documents or sends IM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	remoteembedding "github.com/liuzengh/trpc-agent-service/trpcservice/embedding"
	"go.uber.org/zap"
	agentlog "trpc.group/trpc-go/trpc-agent-go/log"
)

const sampleText = "这是一条不含用户数据的知识库向量连通性测试。"
const maxResponseBytes = remoteembedding.MaxResponseBytes

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "embedding check failed:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("trpc-embeddingcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	envFile := flags.String("env-file", ".env", "private dotenv file; empty uses process environment")
	timeout := flags.Duration("timeout", 30*time.Second, "timeout for one embedding request")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return errors.New("invalid options; use -env-file and/or -timeout")
	}
	if *timeout <= 0 || *timeout > 2*time.Minute {
		return errors.New("timeout must be positive and at most 2m")
	}
	loaded, err := config.LoadDotEnv(*envFile)
	if err != nil {
		return errors.New("cannot load dotenv file; check syntax and file access")
	}
	if !loaded && strings.TrimSpace(*envFile) != "" {
		return errors.New("dotenv file does not exist")
	}
	cfg, err := config.LoadEmbeddingCheckConfigFromEnv()
	if err != nil {
		return err
	}

	// This is a single-operation CLI, never an in-process server component.
	// The pinned framework logs raw provider failures even with retries=0.
	// Suppress those logs here and report only the safe classification below.
	previous := agentlog.ContextDefault
	agentlog.ContextDefault = zap.NewNop().Sugar()
	defer func() { agentlog.ContextDefault = previous }()
	selected, err := remoteembedding.NewRemote(remoteembedding.Config{Model: cfg.Model, BaseURL: cfg.BaseURL, APIKey: cfg.APIKey, Dimensions: cfg.Dimensions})
	if err != nil {
		return err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(selected)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	_, _ = fmt.Fprintf(stdout, "checking embedding provider=openai dimensions=%d requests=1\n", cfg.Dimensions)
	started := time.Now()
	vector, _, err := selected.GetEmbeddingWithUsage(ctx, sampleText)
	if err != nil {
		return safeProviderError(err)
	}
	if err := ctx.Err(); err != nil {
		return safeProviderError(err)
	}
	if err := validateVector(vector, cfg.Dimensions); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "embedding check passed: dimensions=%d latency=%s finite=true nonzero=true\n", len(vector), time.Since(started).Round(time.Millisecond))
	_, _ = fmt.Fprintln(stdout, "Connectivity and vector shape verified; no document import or semantic retrieval validation performed.")
	return nil
}

func safeProviderError(err error) error {
	if errors.Is(err, context.Canceled) {
		return errors.New("embedding request canceled")
	}
	var network net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &network) && network.Timeout()) {
		return errors.New("embedding request timed out; check provider and network")
	}
	var api *remoteembedding.ProviderError
	if errors.As(err, &api) {
		switch api.Kind {
		case "response_limit":
			return errors.New("embedding response exceeds 4 MiB")
		case "dimensions":
			return errors.New("embedding dimension mismatch: configure the actual supported output dimension")
		case "vector":
			return errors.New("embedding must contain finite values and have a finite nonzero norm")
		}
		switch api.HTTPStatus {
		case 401, 403:
			return errors.New("embedding API authentication or permission rejected (401/403)")
		case 404:
			return errors.New("embedding endpoint or model not found (404); a chat endpoint is not an embeddings endpoint")
		case 429:
			return errors.New("embedding API rate limit or quota reached (429); not automatically retried")
		default:
			if api.HTTPStatus >= 100 && api.HTTPStatus <= 599 {
				return fmt.Errorf("embedding API failed: HTTP %d; provider details withheld", api.HTTPStatus)
			}
		}
	}
	return errors.New("embedding request failed or response invalid; raw provider/network details withheld")
}

func validateVector(vector []float64, dimensions int) error {
	if len(vector) != dimensions {
		return fmt.Errorf("embedding dimension mismatch: expected=%d actual=%d", dimensions, len(vector))
	}
	var norm float64
	for _, value := range vector {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return errors.New("embedding contains non-finite values")
		}
		norm = math.Hypot(norm, value)
	}
	if norm == 0 || math.IsInf(norm, 0) {
		return errors.New("embedding must have a finite nonzero norm")
	}
	return nil
}
