// healthcheck is the self-contained probe used by the distroless runtime
// image. It intentionally depends only on the Go standard library: the final
// image has neither a shell nor curl available for a Docker HEALTHCHECK.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type config struct {
	address string
	path    string
	timeout time.Duration
}

func main() {
	if err := run(os.Args[1:], &http.Client{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, client *http.Client) error {
	value := config{}
	flags := flag.NewFlagSet("trpc-healthcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&value.address, "address", "http://127.0.0.1:8080", "runtime HTTP address")
	flags.StringVar(&value.path, "path", "/readyz", "health endpoint path")
	flags.DurationVar(&value.timeout, "timeout", 2*time.Second, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: trpc-healthcheck [-address URL] [-path PATH] [-timeout DURATION]")
	}
	endpoint, err := checkURL(value)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), value.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build health request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("health check %s: %w", endpoint, err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("health check %s: received HTTP %s", endpoint, response.Status)
	}
	return nil
}

func checkURL(value config) (string, error) {
	if value.timeout <= 0 {
		return "", errors.New("health check timeout must be positive")
	}
	base, err := url.Parse(value.address)
	if err != nil || base.Scheme != "http" || base.Host == "" || base.RawQuery != "" || base.Fragment != "" {
		return "", errors.New("health check address must be an HTTP URL without query or fragment")
	}
	if !strings.HasPrefix(value.path, "/") || strings.Contains(value.path, "?") || strings.Contains(value.path, "#") {
		return "", errors.New("health check path must be an absolute path without query or fragment")
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + value.path
	return base.String(), nil
}
