package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/bootstrap"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/memorymigrations"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, out io.Writer) error {
	if len(args) > 0 && args[0] == "probe" {
		flags := flag.NewFlagSet("probe", flag.ContinueOnError)
		flags.SetOutput(out)
		address := flags.String("url", "http://127.0.0.1:8083/readyz", "local readiness endpoint")
		timeout := flags.Duration("timeout", 2*time.Second, "readiness deadline")
		if err := flags.Parse(args[1:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if flags.NArg() == 1 {
			explicit := false
			flags.Visit(func(f *flag.Flag) {
				if f.Name == "url" {
					explicit = true
				}
			})
			if explicit {
				return errors.New("provide one readiness URL")
			}
			*address = flags.Arg(0)
		}
		u, err := url.Parse(*address)
		if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/readyz" || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() || *timeout <= 0 || flags.NArg() > 1 {
			return errors.New("invalid local readiness probe")
		}
		client := &http.Client{Timeout: *timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		response, err := client.Get(u.String())
		if err != nil {
			return errors.New("WORKER_READY=FAIL")
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			return errors.New("WORKER_READY=FAIL")
		}
		_, err = fmt.Fprintln(out, "WORKER_READY=PASS")
		return err
	}
	if len(args) > 0 && args[0] == "prepare-session" {
		flags := flag.NewFlagSet("prepare-session", flag.ContinueOnError)
		flags.SetOutput(out)
		timeout := flags.Duration("timeout", 30*time.Second, "explicit preparation deadline")
		if err := flags.Parse(args[1:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if flags.NArg() != 0 || *timeout <= 0 {
			return errors.New("invalid Session preparation arguments")
		}
		dsn := os.Getenv("SESSION_MIGRATION_DATABASE_URL")
		if dsn == "" {
			return errors.New("SESSION_MIGRATION_DATABASE_URL is required")
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return errors.New("open Session migration database")
		}
		defer pool.Close()
		if err = sessionmigrations.Apply(ctx, pool); err != nil {
			return errors.New("Session preparation failed")
		}
		_, err = fmt.Fprintln(out, "SESSION_PREPARATION=PASS")
		return err
	}
	if len(args) > 0 && args[0] == "prepare-memory" {
		flags := flag.NewFlagSet("prepare-memory", flag.ContinueOnError)
		flags.SetOutput(out)
		timeout := flags.Duration("timeout", 30*time.Second, "explicit preparation deadline")
		if err := flags.Parse(args[1:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if flags.NArg() != 0 || *timeout <= 0 {
			return errors.New("invalid Memory preparation arguments")
		}
		dsn := os.Getenv("MEMORY_MIGRATION_DATABASE_URL")
		if dsn == "" {
			return errors.New("MEMORY_MIGRATION_DATABASE_URL is required")
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return errors.New("open Memory migration database")
		}
		defer pool.Close()
		if err = memorymigrations.Apply(ctx, pool); err != nil {
			return errors.New("Memory preparation failed")
		}
		_, err = fmt.Fprintln(out, "MEMORY_PREPARATION=PASS")
		return err
	}
	flags := flag.NewFlagSet("agent-worker", flag.ContinueOnError)
	flags.SetOutput(out)
	check := flags.Bool("check-config", false, "validate explicit Worker configuration without opening services")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unknown Worker command")
	}
	c, err := bootstrap.LoadConfig()
	if err != nil {
		return err
	}
	if *check {
		_, err = fmt.Fprintln(out, "WORKER_CONFIG=PASS")
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, stopStartup := context.WithTimeout(ctx, c.Timing.StartupTimeout.Value())
	app, err := bootstrap.New(startup, c)
	stopStartup()
	if err != nil {
		return err
	}
	return app.Run(ctx)
}
